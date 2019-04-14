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

#ifdef ATOM
#define PREFETCH	prefetchnta
#define PREFETCHW	prefetcht0
#define PREFETCHSIZE	(8 * 6)
#endif

#define STACKSIZE	16

#define M		 4 + STACKSIZE(%esp)
#define N		 8 + STACKSIZE(%esp)
#define ALPHA		16 + STACKSIZE(%esp)
#define A		24 + STACKSIZE(%esp)
#define STACK_LDA	28 + STACKSIZE(%esp)
#define STACK_X		32 + STACKSIZE(%esp)
#define STACK_INCX	36 + STACKSIZE(%esp)
#define Y		40 + STACKSIZE(%esp)
#define STACK_INCY	44 + STACKSIZE(%esp)
#define BUFFER		48 + STACKSIZE(%esp)

#define I	%eax
#define J	%ebx

#define INCX	%ecx
#define INCY	J

#define A1	%esi
#define X	%edx
#define Y1	%edi
#define LDA	%ebp

	PROLOGUE

	pushl	%ebp
	pushl	%edi
	pushl	%esi
	pushl	%ebx

	PROFCODE

	movl	STACK_LDA,  LDA
	movl	STACK_X,    X
	movl	STACK_INCX, INCX

	leal	(,INCX, SIZE), INCX
	leal	(,LDA,  SIZE), LDA

	subl	$-16 * SIZE, A

	cmpl	$0, N
	jle	.L999
	cmpl	$0, M
	jle	.L999

	movl	BUFFER, Y1

	pxor	%xmm7, %xmm7

	movl	M,   %eax
	addl	$16, %eax
	sarl	$4,  %eax
	ALIGN_3

.L01:
	movapd	%xmm7,  0 * SIZE(Y1)
	movapd	%xmm7,  2 * SIZE(Y1)
	movapd	%xmm7,  4 * SIZE(Y1)
	movapd	%xmm7,  6 * SIZE(Y1)
	movapd	%xmm7,  8 * SIZE(Y1)
	movapd	%xmm7, 10 * SIZE(Y1)
	movapd	%xmm7, 12 * SIZE(Y1)
	movapd	%xmm7, 14 * SIZE(Y1)
	subl	$-16 * SIZE, Y1
	decl	%eax
	jg	.L01
	ALIGN_3

.L10:
	movl	N,  J
	sarl	$1, J
	jle	.L20
	ALIGN_3

.L11:
	movl	BUFFER, Y1
	addl	$16 * SIZE, Y1

	movl	A,  A1
	leal	(A1,  LDA, 2), %eax
	movl	%eax, A

	movsd	(X), %xmm6
	addl	INCX, X
	movsd	(X), %xmm7
	addl	INCX, X

	movsd	ALPHA, %xmm0

	mulsd	%xmm0, %xmm6
	mulsd	%xmm0, %xmm7

	movsd	 -16 * SIZE(Y1), %xmm0
	movsd	 -15 * SIZE(Y1), %xmm1

	movl	M,   I
	sarl	$3,  I
	jle	.L15

	movsd	 -16 * SIZE(A1), %xmm2
	movsd	 -15 * SIZE(A1), %xmm3
	movsd	 -16 * SIZE(A1, LDA), %xmm4
	movsd	 -15 * SIZE(A1, LDA), %xmm5

	mulsd	 %xmm6, %xmm2
	mulsd	 %xmm6, %xmm3

	decl	 I
	jle	 .L14
	ALIGN_3

.L13:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE + 0) * SIZE(A1)
#endif

	mulsd	 %xmm7, %xmm4
	addsd	 %xmm2, %xmm0
	movsd	 -14 * SIZE(A1), %xmm2
	mulsd	 %xmm7, %xmm5
	addsd	 %xmm3, %xmm1
	movsd	 -13 * SIZE(A1), %xmm3

	addsd	 %xmm4, %xmm0
	movsd	 -14 * SIZE(A1, LDA), %xmm4
	mulsd	 %xmm6, %xmm2
	addsd	 %xmm5, %xmm1
	movsd	 -13 * SIZE(A1, LDA), %xmm5
	mulsd	 %xmm6, %xmm3

	movlpd	 %xmm0, -16 * SIZE(Y1)
	movsd	 -14 * SIZE(Y1), %xmm0
	movlpd	 %xmm1, -15 * SIZE(Y1)
	movsd	 -13 * SIZE(Y1), %xmm1

	mulsd	 %xmm7, %xmm4
	addsd	 %xmm2, %xmm0
	movsd	 -12 * SIZE(A1), %xmm2
	mulsd	 %xmm7, %xmm5
	addsd	 %xmm3, %xmm1
	movsd	 -11 * SIZE(A1), %xmm3

	addsd	 %xmm4, %xmm0
	movsd	 -12 * SIZE(A1, LDA), %xmm4
	mulsd	 %xmm6, %xmm2
	addsd	 %xmm5, %xmm1
	movsd	 -11 * SIZE(A1, LDA), %xmm5
	mulsd	 %xmm6, %xmm3

	movlpd	 %xmm0, -14 * SIZE(Y1)
	movsd	 -12 * SIZE(Y1), %xmm0
	movlpd	 %xmm1, -13 * SIZE(Y1)
	movsd	 -11 * SIZE(Y1), %xmm1

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE + 0) * SIZE(A1, LDA)
#endif

	mulsd	 %xmm7, %xmm4
	addsd	 %xmm2, %xmm0
	movsd	 -10 * SIZE(A1), %xmm2
	mulsd	 %xmm7, %xmm5
	addsd	 %xmm3, %xmm1
	movsd	  -9 * SIZE(A1), %xmm3

	addsd	 %xmm4, %xmm0
	movsd	 -10 * SIZE(A1, LDA), %xmm4
	mulsd	 %xmm6, %xmm2
	addsd	 %xmm5, %xmm1
	movsd	  -9 * SIZE(A1, LDA), %xmm5
	mulsd	 %xmm6, %xmm3

	movlpd	 %xmm0, -12 * SIZE(Y1)
	movsd	 -10 * SIZE(Y1), %xmm0
	movlpd	 %xmm1, -11 * SIZE(Y1)
	movsd	  -9 * SIZE(Y1), %xmm1

	mulsd	 %xmm7, %xmm4
	addsd	 %xmm2, %xmm0
	movsd	  -8 * SIZE(A1), %xmm2
	mulsd	 %xmm7, %xmm5
	addsd	 %xmm3, %xmm1
	movsd	  -7 * SIZE(A1), %xmm3

	addsd	 %xmm4, %xmm0
	movsd	  -8 * SIZE(A1, LDA), %xmm4
	mulsd	 %xmm6, %xmm2
	addsd	 %xmm5, %xmm1
	movsd	  -7 * SIZE(A1, LDA), %xmm5
	mulsd	 %xmm6, %xmm3

	movlpd	 %xmm0, -10 * SIZE(Y1)
	movsd	  -8 * SIZE(Y1), %xmm0
	movlpd	 %xmm1,  -9 * SIZE(Y1)
	movsd	  -7 * SIZE(Y1), %xmm1

	subl	 $-8 * SIZE, A1
	subl	 $-8 * SIZE, Y1

	subl	 $1, I
	BRANCH
	jg	.L13
	ALIGN_3

.L14:
	mulsd	 %xmm7, %xmm4
	addsd	 %xmm2, %xmm0
	movsd	 -14 * SIZE(A1), %xmm2
	mulsd	 %xmm7, %xmm5
	addsd	 %xmm3, %xmm1
	movsd	 -13 * SIZE(A1), %xmm3

	addsd	 %xmm4, %xmm0
	movsd	 -14 * SIZE(A1, LDA), %xmm4
	mulsd	 %xmm6, %xmm2
	addsd	 %xmm5, %xmm1
	movsd	 -13 * SIZE(A1, LDA), %xmm5
	mulsd	 %xmm6, %xmm3

	movlpd	 %xmm0, -16 * SIZE(Y1)
	movsd	 -14 * SIZE(Y1), %xmm0
	movlpd	 %xmm1, -15 * SIZE(Y1)
	movsd	 -13 * SIZE(Y1), %xmm1

	mulsd	 %xmm7, %xmm4
	addsd	 %xmm2, %xmm0
	movsd	 -12 * SIZE(A1), %xmm2
	mulsd	 %xmm7, %xmm5
	addsd	 %xmm3, %xmm1
	movsd	 -11 * SIZE(A1), %xmm3

	addsd	 %xmm4, %xmm0
	movsd	 -12 * SIZE(A1, LDA), %xmm4
	mulsd	 %xmm6, %xmm2
	addsd	 %xmm5, %xmm1
	movsd	 -11 * SIZE(A1, LDA), %xmm5
	mulsd	 %xmm6, %xmm3

	movlpd	 %xmm0, -14 * SIZE(Y1)
	movsd	 -12 * SIZE(Y1), %xmm0
	movlpd	 %xmm1, -13 * SIZE(Y1)
	movsd	 -11 * SIZE(Y1), %xmm1

	mulsd	 %xmm7, %xmm4
	addsd	 %xmm2, %xmm0
	movsd	 -10 * SIZE(A1), %xmm2
	mulsd	 %xmm7, %xmm5
	addsd	 %xmm3, %xmm1
	movsd	  -9 * SIZE(A1), %xmm3

	addsd	 %xmm4, %xmm0
	movsd	 -10 * SIZE(A1, LDA), %xmm4
	mulsd	 %xmm6, %xmm2
	addsd	 %xmm5, %xmm1
	movsd	  -9 * SIZE(A1, LDA), %xmm5
	mulsd	 %xmm6, %xmm3

	movlpd	 %xmm0, -12 * SIZE(Y1)
	movsd	 -10 * SIZE(Y1), %xmm0
	movlpd	 %xmm1, -11 * SIZE(Y1)
	movsd	  -9 * SIZE(Y1), %xmm1

	mulsd	 %xmm7, %xmm4
	addsd	 %xmm2, %xmm0
	mulsd	 %xmm7, %xmm5
	addsd	 %xmm3, %xmm1

	addsd	 %xmm4, %xmm0
	addsd	 %xmm5, %xmm1

	movlpd	 %xmm0, -10 * SIZE(Y1)
	movsd	  -8 * SIZE(Y1), %xmm0
	movlpd	 %xmm1,  -9 * SIZE(Y1)
	movsd	  -7 * SIZE(Y1), %xmm1

	subl	 $-8 * SIZE, A1
	subl	 $-8 * SIZE, Y1
	ALIGN_3

.L15:
	testl	$4, M
	je	.L16

	movsd	 -16 * SIZE(A1), %xmm2
	movsd	 -15 * SIZE(A1), %xmm3
	movsd	 -16 * SIZE(A1, LDA), %xmm4
	movsd	 -15 * SIZE(A1, LDA), %xmm5

	mulsd	 %xmm6, %xmm2
	mulsd	 %xmm6, %xmm3

	mulsd	 %xmm7, %xmm4
	addsd	 %xmm2, %xmm0
	movsd	 -14 * SIZE(A1), %xmm2
	mulsd	 %xmm7, %xmm5
	addsd	 %xmm3, %xmm1
	movsd	 -13 * SIZE(A1), %xmm3

	addsd	 %xmm4, %xmm0
	movsd	 -14 * SIZE(A1, LDA), %xmm4
	mulsd	 %xmm6, %xmm2
	addsd	 %xmm5, %xmm1
	movsd	 -13 * SIZE(A1, LDA), %xmm5
	mulsd	 %xmm6, %xmm3

	movlpd	 %xmm0, -16 * SIZE(Y1)
	movsd	 -14 * SIZE(Y1), %xmm0
	movlpd	 %xmm1, -15 * SIZE(Y1)
	movsd	 -13 * SIZE(Y1), %xmm1

	mulsd	 %xmm7, %xmm4
	addsd	 %xmm2, %xmm0
	mulsd	 %xmm7, %xmm5
	addsd	 %xmm3, %xmm1

	addsd	 %xmm4, %xmm0
	addsd	 %xmm5, %xmm1

	movlpd	 %xmm0, -14 * SIZE(Y1)
	movsd	 -12 * SIZE(Y1), %xmm0
	movlpd	 %xmm1, -13 * SIZE(Y1)
	movsd	 -11 * SIZE(Y1), %xmm1

	addl	 $4 * SIZE, A1
	addl	 $4 * SIZE, Y1
	ALIGN_3

.L16:
	testl	$2, M
	je	.L17

	movsd	 -16 * SIZE(A1), %xmm2
	movsd	 -15 * SIZE(A1), %xmm3
	movsd	 -16 * SIZE(A1, LDA), %xmm4
	movsd	 -15 * SIZE(A1, LDA), %xmm5

	mulsd	 %xmm6, %xmm2
	mulsd	 %xmm6, %xmm3

	mulsd	 %xmm7, %xmm4
	addsd	 %xmm2, %xmm0
	mulsd	 %xmm7, %xmm5
	addsd	 %xmm3, %xmm1

	addsd	 %xmm4, %xmm0
	addsd	 %xmm5, %xmm1

	movlpd	 %xmm0, -16 * SIZE(Y1)
	movsd	 -14 * SIZE(Y1), %xmm0
	movlpd	 %xmm1, -15 * SIZE(Y1)

	addl	 $2 * SIZE, A1
	addl	 $2 * SIZE, Y1
	ALIGN_3

.L17:
	testl	$1, M
	je	.L19

	movsd	 -16 * SIZE(A1), %xmm2
	movsd	 -16 * SIZE(A1, LDA), %xmm3

	movsd	 -16 * SIZE(Y1), %xmm0

	mulsd	 %xmm6, %xmm2
	addsd	 %xmm2, %xmm0
	mulsd	 %xmm7, %xmm3
	addsd	 %xmm3, %xmm0

	movsd	 %xmm0, -16 * SIZE(Y1)
	ALIGN_3

.L19:
	decl	J
	jg	.L11
	ALIGN_4

.L20:
	testl	$1, N
	jle	.L990

	movl	BUFFER, Y1
	addl	$16 * SIZE, Y1

	movl	A,  A1
	leal	(A1,  LDA, 2), %eax
	movl	%eax, A

	movsd	(X), %xmm6
	addl	INCX, X
	movsd	(X), %xmm7
	addl	INCX, X

	movsd	ALPHA, %xmm0

	mulsd	%xmm0, %xmm6
	mulsd	%xmm0, %xmm7

	movsd	 -16 * SIZE(Y1), %xmm0
	movsd	 -15 * SIZE(Y1), %xmm1
	movsd	 -14 * SIZE(Y1), %xmm4
	movsd	 -13 * SIZE(Y1), %xmm5

	movl	M,   I
	sarl	$3,  I
	jle	.L25

	movsd	 -16 * SIZE(A1), %xmm2
	movsd	 -15 * SIZE(A1), %xmm3

	mulsd	 %xmm6, %xmm2
	mulsd	 %xmm6, %xmm3

	decl	 I
	jle	 .L24
	ALIGN_3

.L23:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE + 0) * SIZE(A1)
#endif

	addsd	 %xmm2, %xmm0
	movsd	 -14 * SIZE(A1), %xmm2
	addsd	 %xmm3, %xmm1
	movsd	 -13 * SIZE(A1), %xmm3

	mulsd	 %xmm6, %xmm2
	movlpd	 %xmm0, -16 * SIZE(Y1)
	movsd	 -12 * SIZE(Y1), %xmm0
	mulsd	 %xmm6, %xmm3
	movlpd	 %xmm1, -15 * SIZE(Y1)
	movsd	 -11 * SIZE(Y1), %xmm1

	addsd	 %xmm2, %xmm4
	movsd	 -12 * SIZE(A1), %xmm2
	addsd	 %xmm3, %xmm5
	movsd	 -11 * SIZE(A1), %xmm3

	mulsd	 %xmm6, %xmm2
	movlpd	 %xmm4, -14 * SIZE(Y1)
	movsd	 -10 * SIZE(Y1), %xmm4
	mulsd	 %xmm6, %xmm3
	movlpd	 %xmm5, -13 * SIZE(Y1)
	movsd	  -9 * SIZE(Y1), %xmm5

	addsd	 %xmm2, %xmm0
	movsd	 -10 * SIZE(A1), %xmm2
	addsd	 %xmm3, %xmm1
	movsd	  -9 * SIZE(A1), %xmm3

	mulsd	 %xmm6, %xmm2
	movlpd	 %xmm0, -12 * SIZE(Y1)
	movsd	  -8 * SIZE(Y1), %xmm0
	mulsd	 %xmm6, %xmm3
	movlpd	 %xmm1, -11 * SIZE(Y1)
	movsd	  -7 * SIZE(Y1), %xmm1

	addsd	 %xmm2, %xmm4
	movsd	  -8 * SIZE(A1), %xmm2
	addsd	 %xmm3, %xmm5
	movsd	  -7 * SIZE(A1), %xmm3

	mulsd	 %xmm6, %xmm2
	movlpd	 %xmm4, -10 * SIZE(Y1)
	movsd	  -6 * SIZE(Y1), %xmm4
	mulsd	 %xmm6, %xmm3
	movlpd	 %xmm5,  -9 * SIZE(Y1)
	movsd	  -5 * SIZE(Y1), %xmm5

	subl	 $-8 * SIZE, A1
	subl	 $-8 * SIZE, Y1

	subl	 $1, I
	BRANCH
	jg	.L23
	ALIGN_3

.L24:
	addsd	 %xmm2, %xmm0
	movsd	 -14 * SIZE(A1), %xmm2
	addsd	 %xmm3, %xmm1
	movsd	 -13 * SIZE(A1), %xmm3

	mulsd	 %xmm6, %xmm2
	movlpd	 %xmm0, -16 * SIZE(Y1)
	movsd	 -12 * SIZE(Y1), %xmm0
	mulsd	 %xmm6, %xmm3
	movlpd	 %xmm1, -15 * SIZE(Y1)
	movsd	 -11 * SIZE(Y1), %xmm1

	addsd	 %xmm2, %xmm4
	movsd	 -12 * SIZE(A1), %xmm2
	addsd	 %xmm3, %xmm5
	movsd	 -11 * SIZE(A1), %xmm3

	mulsd	 %xmm6, %xmm2
	movlpd	 %xmm4, -14 * SIZE(Y1)
	movsd	 -10 * SIZE(Y1), %xmm4
	mulsd	 %xmm6, %xmm3
	movlpd	 %xmm5, -13 * SIZE(Y1)
	movsd	  -9 * SIZE(Y1), %xmm5

	addsd	 %xmm2, %xmm0
	movsd	 -10 * SIZE(A1), %xmm2
	addsd	 %xmm3, %xmm1
	movsd	  -9 * SIZE(A1), %xmm3

	mulsd	 %xmm6, %xmm2
	movlpd	 %xmm0, -12 * SIZE(Y1)
	mulsd	 %xmm6, %xmm3
	movlpd	 %xmm1, -11 * SIZE(Y1)

	addsd	 %xmm2, %xmm4
	movsd	  -8 * SIZE(Y1), %xmm0
	addsd	 %xmm3, %xmm5
	movsd	  -7 * SIZE(Y1), %xmm1

	movlpd	 %xmm4, -10 * SIZE(Y1)
	movsd	  -6 * SIZE(Y1), %xmm4
	movlpd	 %xmm5,  -9 * SIZE(Y1)
	movsd	  -5 * SIZE(Y1), %xmm5

	subl	 $-8 * SIZE, A1
	subl	 $-8 * SIZE, Y1
	ALIGN_3

.L25:
	testl	$4, M
	je	.L26

	movsd	 -16 * SIZE(A1), %xmm2
	movsd	 -15 * SIZE(A1), %xmm3
	mulsd	 %xmm6, %xmm2
	mulsd	 %xmm6, %xmm3

	addsd	 %xmm2, %xmm0
	movsd	 -14 * SIZE(A1), %xmm2
	addsd	 %xmm3, %xmm1
	movsd	 -13 * SIZE(A1), %xmm3

	mulsd	 %xmm6, %xmm2
	movlpd	 %xmm0, -16 * SIZE(Y1)
	movsd	 -12 * SIZE(Y1), %xmm0
	mulsd	 %xmm6, %xmm3
	movlpd	 %xmm1, -15 * SIZE(Y1)
	movsd	 -11 * SIZE(Y1), %xmm1

	addsd	 %xmm2, %xmm4
	addsd	 %xmm3, %xmm5

	movlpd	 %xmm4, -14 * SIZE(Y1)
	movlpd	 %xmm5, -13 * SIZE(Y1)

	addl	 $4 * SIZE, A1
	addl	 $4 * SIZE, Y1
	ALIGN_3

.L26:
	testl	$2, M
	je	.L27

	movsd	 -16 * SIZE(A1), %xmm2
	movsd	 -15 * SIZE(A1), %xmm3

	mulsd	 %xmm6, %xmm2
	mulsd	 %xmm6, %xmm3
	addsd	 %xmm2, %xmm0
	addsd	 %xmm3, %xmm1

	movlpd	 %xmm0, -16 * SIZE(Y1)
	movsd	 -14 * SIZE(Y1), %xmm0
	movlpd	 %xmm1, -15 * SIZE(Y1)

	addl	 $2 * SIZE, A1
	addl	 $2 * SIZE, Y1
	ALIGN_3

.L27:
	testl	$1, M
	je	.L990

	movsd	 -16 * SIZE(A1), %xmm2
	movsd	 -16 * SIZE(Y1), %xmm0

	mulsd	 %xmm6, %xmm2
	addsd	 %xmm2, %xmm0

	movsd	 %xmm0, -16 * SIZE(Y1)
	ALIGN_3

.L990:
	movl	Y,   Y1
	movl	BUFFER, X
	movl	Y1,  A1

	movl	STACK_INCY, INCY
	sall	$BASE_SHIFT, INCY

	movl	M,   %eax
	sarl	$3,  %eax
	jle	.L994
	ALIGN_3

.L992:
	movsd	(Y1), %xmm0
	addl	INCY, Y1
	movsd	(Y1), %xmm1
	addl	INCY, Y1
	movsd	(Y1), %xmm2
	addl	INCY, Y1
	movsd	(Y1), %xmm3
	addl	INCY, Y1
	movsd	(Y1), %xmm4
	addl	INCY, Y1
	movsd	(Y1), %xmm5
	addl	INCY, Y1
	movsd	(Y1), %xmm6
	addl	INCY, Y1
	movsd	(Y1), %xmm7
	addl	INCY, Y1

	addsd	0 * SIZE(X), %xmm0
	addsd	1 * SIZE(X), %xmm1
	addsd	2 * SIZE(X), %xmm2
	addsd	3 * SIZE(X), %xmm3
	addsd	4 * SIZE(X), %xmm4
	addsd	5 * SIZE(X), %xmm5
	addsd	6 * SIZE(X), %xmm6
	addsd	7 * SIZE(X), %xmm7

	movlpd	%xmm0, (A1)
	addl	INCY, A1
	movlpd	%xmm1, (A1)
	addl	INCY, A1
	movlpd	%xmm2, (A1)
	addl	INCY, A1
	movlpd	%xmm3, (A1)
	addl	INCY, A1
	movlpd	%xmm4, (A1)
	addl	INCY, A1
	movlpd	%xmm5, (A1)
	addl	INCY, A1
	movlpd	%xmm6, (A1)
	addl	INCY, A1
	movlpd	%xmm7, (A1)
	addl	INCY, A1

	addl	$8 * SIZE, X
	decl	%eax
	jg	.L992
	ALIGN_3

.L994:
	testl	$7, M
	jle	.L999

	testl	$4, M
	jle	.L995

	movsd	(Y1), %xmm0
	addl	INCY, Y1
	movsd	(Y1), %xmm1
	addl	INCY, Y1
	movsd	(Y1), %xmm2
	addl	INCY, Y1
	movsd	(Y1), %xmm3
	addl	INCY, Y1

	addsd	0 * SIZE(X), %xmm0
	addsd	1 * SIZE(X), %xmm1
	addsd	2 * SIZE(X), %xmm2
	addsd	3 * SIZE(X), %xmm3

	movlpd	%xmm0, (A1)
	addl	INCY, A1
	movlpd	%xmm1, (A1)
	addl	INCY, A1
	movlpd	%xmm2, (A1)
	addl	INCY, A1
	movlpd	%xmm3, (A1)
	addl	INCY, A1

	addl	$4 * SIZE, X
	ALIGN_3

.L995:
	testl	$2, M
	jle	.L996

	movsd	(Y1), %xmm0
	addl	INCY, Y1
	movsd	(Y1), %xmm1
	addl	INCY, Y1

	addsd	0 * SIZE(X), %xmm0
	addsd	1 * SIZE(X), %xmm1

	movlpd	%xmm0, (A1)
	addl	INCY, A1
	movlpd	%xmm1, (A1)
	addl	INCY, A1

	addl	$2 * SIZE, X
	ALIGN_3

.L996:
	testl	$1, M
	jle	.L999

	movsd	(Y1), %xmm0

	addsd	0 * SIZE(X), %xmm0

	movlpd	%xmm0, (A1)
	ALIGN_3

.L999:
	popl	%ebx
	popl	%esi
	popl	%edi
	popl	%ebp
	ret

	EPILOGUE
