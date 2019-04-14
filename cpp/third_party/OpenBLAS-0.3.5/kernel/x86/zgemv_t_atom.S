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
#define ALPHA_R		16 + STACKSIZE(%esp)
#define ALPHA_I		24 + STACKSIZE(%esp)
#define A		32 + STACKSIZE(%esp)
#define STACK_LDA	36 + STACKSIZE(%esp)
#define STACK_X		40 + STACKSIZE(%esp)
#define STACK_INCX	44 + STACKSIZE(%esp)
#define Y		48 + STACKSIZE(%esp)
#define STACK_INCY	52 + STACKSIZE(%esp)
#define BUFFER		56 + STACKSIZE(%esp)

#define I	%eax
#define J	%ebx

#define INCX	J
#define INCY	%ecx

#define A1	%esi
#define X	%edx
#define Y1	%edi
#define LDA	%ebp

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

	pushl	%ebp
	pushl	%edi
	pushl	%esi
	pushl	%ebx

	PROFCODE

	movl	STACK_LDA,  LDA
	movl	STACK_X,    X
	movl	STACK_INCX, INCX
	movl	STACK_INCY, INCY

	sall	$ZBASE_SHIFT, INCX
	sall	$ZBASE_SHIFT, INCY
	sall	$ZBASE_SHIFT, LDA

	subl	$-16 * SIZE, A

	cmpl	$0, N
	jle	.L999
	cmpl	$0, M
	jle	.L999

	movl	BUFFER, Y1

	movl	M,  I
	sarl	$2, I
	jle	.L05
	ALIGN_4

.L02:
	movsd	0 * SIZE(X), %xmm0
	movhpd	1 * SIZE(X), %xmm0
	addl	INCX, X

	movsd	0 * SIZE(X), %xmm1
	movhpd	1 * SIZE(X), %xmm1
	addl	INCX, X

	movsd	0 * SIZE(X), %xmm2
	movhpd	1 * SIZE(X), %xmm2
	addl	INCX, X

	movsd	0 * SIZE(X), %xmm3
	movhpd	1 * SIZE(X), %xmm3
	addl	INCX, X

	movapd	%xmm0, 0 * SIZE(Y1)
	movapd	%xmm1, 2 * SIZE(Y1)
	movapd	%xmm2, 4 * SIZE(Y1)
	movapd	%xmm3, 6 * SIZE(Y1)

	addl	$8 * SIZE, Y1
	decl	I
	jg	.L02
	ALIGN_4

.L05:
	movl	M,  I
	andl	$3, I
	jle	.L10
	ALIGN_2

.L06:
	movsd	0 * SIZE(X), %xmm0
	movhpd	1 * SIZE(X), %xmm0
	addl	INCX, X

	movapd	%xmm0, 0 * SIZE(Y1)
	addl	$2 * SIZE, Y1
	decl	I
	jg	.L06
	ALIGN_4

.L10:
	movl	Y, Y1

	movl	N,  J
	ALIGN_3

.L11:
	movl	BUFFER, X
	addl	$16 * SIZE, X

	movl	A, A1
	addl	LDA, A

	xorps	%xmm0, %xmm0
	xorps	%xmm1, %xmm1

	movsd	-16 * SIZE(X), %xmm2
	movsd	-15 * SIZE(X), %xmm3

	movl	M,   I
	sarl	$2,  I
	jle	.L15

	movsd	-16 * SIZE(A1), %xmm4
	movsd	-15 * SIZE(A1), %xmm5

	movapd	%xmm4, %xmm6
	mulsd	%xmm2, %xmm4
	mulsd	%xmm3, %xmm6

	decl	I
	jle	.L13
	ALIGN_4

.L12:
#ifdef PREFETCH
	PREFETCH PREFETCHSIZE * SIZE(A1)
#endif

	movapd	%xmm5, %xmm7
	mulsd	%xmm3, %xmm5
	movsd	-13 * SIZE(X), %xmm3
	ADD1	%xmm4, %xmm0
	movsd	-14 * SIZE(A1), %xmm4
	mulsd	%xmm2, %xmm7
	movsd	-14 * SIZE(X), %xmm2
	ADD2	%xmm6, %xmm1

	movapd	%xmm4, %xmm6
	mulsd	%xmm2, %xmm4
	ADD3	%xmm5, %xmm0
	movsd	-13 * SIZE(A1), %xmm5
	mulsd	%xmm3, %xmm6
	ADD4	%xmm7, %xmm1

	movapd	%xmm5, %xmm7
	mulsd	%xmm3, %xmm5
	movsd	-11 * SIZE(X), %xmm3
	ADD1	%xmm4, %xmm0
	movsd	-12 * SIZE(A1), %xmm4
	mulsd	%xmm2, %xmm7
	movsd	-12 * SIZE(X), %xmm2
	ADD2	%xmm6, %xmm1

	movapd	%xmm4, %xmm6
	mulsd	%xmm2, %xmm4
	ADD3	%xmm5, %xmm0
	movsd	-11 * SIZE(A1), %xmm5
	mulsd	%xmm3, %xmm6
	ADD4	%xmm7, %xmm1

	movapd	%xmm5, %xmm7
	mulsd	%xmm3, %xmm5
	movsd	 -9 * SIZE(X), %xmm3
	ADD1	%xmm4, %xmm0
	movsd	-10 * SIZE(A1), %xmm4
	mulsd	%xmm2, %xmm7
	movsd	-10 * SIZE(X), %xmm2
	ADD2	%xmm6, %xmm1

	movapd	%xmm4, %xmm6
	mulsd	%xmm2, %xmm4
	ADD3	%xmm5, %xmm0
	movsd	 -9 * SIZE(A1), %xmm5
	mulsd	%xmm3, %xmm6
	ADD4	%xmm7, %xmm1

	movapd	%xmm5, %xmm7
	mulsd	%xmm3, %xmm5
	movsd	 -7 * SIZE(X), %xmm3
	ADD1	%xmm4, %xmm0
	movsd	 -8 * SIZE(A1), %xmm4
	mulsd	%xmm2, %xmm7
	movsd	 -8 * SIZE(X), %xmm2
	ADD2	%xmm6, %xmm1

	movapd	%xmm4, %xmm6
	mulsd	%xmm2, %xmm4
	ADD3	%xmm5, %xmm0
	movsd	 -7 * SIZE(A1), %xmm5
	mulsd	%xmm3, %xmm6
	ADD4	%xmm7, %xmm1

	addl	$8 * SIZE, A1
	addl	$8 * SIZE, X

	decl	I
	jg	.L12
	ALIGN_4

.L13:
	movapd	%xmm5, %xmm7
	mulsd	%xmm3, %xmm5
	movsd	-13 * SIZE(X), %xmm3
	ADD1	%xmm4, %xmm0
	movsd	-14 * SIZE(A1), %xmm4
	mulsd	%xmm2, %xmm7
	movsd	-14 * SIZE(X), %xmm2
	ADD2	%xmm6, %xmm1

	movapd	%xmm4, %xmm6
	mulsd	%xmm2, %xmm4
	ADD3	%xmm5, %xmm0
	movsd	-13 * SIZE(A1), %xmm5
	mulsd	%xmm3, %xmm6
	ADD4	%xmm7, %xmm1

	movapd	%xmm5, %xmm7
	mulsd	%xmm3, %xmm5
	movsd	-11 * SIZE(X), %xmm3
	ADD1	%xmm4, %xmm0
	movsd	-12 * SIZE(A1), %xmm4
	mulsd	%xmm2, %xmm7
	movsd	-12 * SIZE(X), %xmm2
	ADD2	%xmm6, %xmm1

	movapd	%xmm4, %xmm6
	mulsd	%xmm2, %xmm4
	ADD3	%xmm5, %xmm0
	movsd	-11 * SIZE(A1), %xmm5
	mulsd	%xmm3, %xmm6
	ADD4	%xmm7, %xmm1

	movapd	%xmm5, %xmm7
	mulsd	%xmm3, %xmm5
	movsd	 -9 * SIZE(X), %xmm3
	ADD1	%xmm4, %xmm0
	movsd	-10 * SIZE(A1), %xmm4
	mulsd	%xmm2, %xmm7
	movsd	-10 * SIZE(X), %xmm2
	ADD2	%xmm6, %xmm1

	movapd	%xmm4, %xmm6
	mulsd	%xmm2, %xmm4
	ADD3	%xmm5, %xmm0
	movsd	 -9 * SIZE(A1), %xmm5
	mulsd	%xmm3, %xmm6
	ADD4	%xmm7, %xmm1

	movapd	%xmm5, %xmm7
	mulsd	%xmm3, %xmm5
	movsd	 -7 * SIZE(X), %xmm3
	ADD1	%xmm4, %xmm0
	mulsd	%xmm2, %xmm7
	movsd	 -8 * SIZE(X), %xmm2
	ADD2	%xmm6, %xmm1

	ADD3	%xmm5, %xmm0
	ADD4	%xmm7, %xmm1

	addl	$8 * SIZE, A1
	addl	$8 * SIZE, X
	ALIGN_4

.L15:
	testl	$2, M
	jle	.L17

	movsd	-16 * SIZE(A1), %xmm4
	movsd	-15 * SIZE(A1), %xmm5

	movapd	%xmm4, %xmm6
	mulsd	%xmm2, %xmm4
	mulsd	%xmm3, %xmm6

	movapd	%xmm5, %xmm7
	mulsd	%xmm3, %xmm5
	movsd	-13 * SIZE(X), %xmm3
	ADD1	%xmm4, %xmm0
	movsd	-14 * SIZE(A1), %xmm4
	mulsd	%xmm2, %xmm7
	movsd	-14 * SIZE(X), %xmm2
	ADD2	%xmm6, %xmm1

	movapd	%xmm4, %xmm6
	mulsd	%xmm2, %xmm4
	ADD3	%xmm5, %xmm0
	movsd	-13 * SIZE(A1), %xmm5
	mulsd	%xmm3, %xmm6
	ADD4	%xmm7, %xmm1

	movapd	%xmm5, %xmm7
	mulsd	%xmm3, %xmm5
	movsd	-11 * SIZE(X), %xmm3
	ADD1	%xmm4, %xmm0
	mulsd	%xmm2, %xmm7
	movsd	-12 * SIZE(X), %xmm2
	ADD2	%xmm6, %xmm1

	ADD3	%xmm5, %xmm0
	ADD4	%xmm7, %xmm1

	addl	$4 * SIZE, A1
	ALIGN_4

.L17:
	testl	$1, M
	jle	.L18

	movsd	-16 * SIZE(A1), %xmm4
	movsd	-15 * SIZE(A1), %xmm5

	movapd	%xmm4, %xmm6
	mulsd	%xmm2, %xmm4
	mulsd	%xmm3, %xmm6

	movapd	%xmm5, %xmm7
	mulsd	%xmm3, %xmm5
	ADD1	%xmm4, %xmm0
	mulsd	%xmm2, %xmm7
	ADD2	%xmm6, %xmm1

	ADD3	%xmm5, %xmm0
	ADD4	%xmm7, %xmm1
	ALIGN_4

.L18:
	movsd	 0 * SIZE(Y1), %xmm4
	movapd	%xmm0, %xmm2
	mulsd	ALPHA_R, %xmm0
	movsd	 1 * SIZE(Y1), %xmm5
	movapd	%xmm1, %xmm3
	mulsd	ALPHA_R, %xmm1

	mulsd	ALPHA_I, %xmm2
	mulsd	ALPHA_I, %xmm3

	addsd	%xmm2, %xmm1
	subsd	%xmm3, %xmm0
	addsd	%xmm4, %xmm0
	addsd	%xmm5, %xmm1

	movlpd	%xmm0,  0 * SIZE(Y1)
	movlpd	%xmm1,  1 * SIZE(Y1)

	addl	INCY, Y1

	decl	J
	jg	.L11
	ALIGN_4

.L999:
	popl	%ebx
	popl	%esi
	popl	%edi
	popl	%ebp
	ret

	EPILOGUE
