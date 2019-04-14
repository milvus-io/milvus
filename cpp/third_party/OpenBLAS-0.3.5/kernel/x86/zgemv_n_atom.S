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

#define INCX	%ecx
#define INCY	J

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

	sall	$ZBASE_SHIFT, INCX
	sall	$ZBASE_SHIFT, LDA

	subl	$-16 * SIZE, A

	cmpl	$0, N
	jle	.L999
	cmpl	$0, M
	jle	.L999

	movl	BUFFER, Y1

	movl	N,  J

	pxor	%xmm7, %xmm7

	movl	M,  %eax
	addl	$8, %eax
	sarl	$3, %eax
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
	movl	BUFFER, Y1
	addl	$16 * SIZE, Y1

	movl	A,  A1
	addl	LDA, A

	movsd	0 * SIZE(X), %xmm6
	movsd	1 * SIZE(X), %xmm7
	addl	INCX, X

	movapd	%xmm6,   %xmm2
	mulsd	ALPHA_R, %xmm6
	mulsd	ALPHA_I, %xmm2
	movapd	%xmm7,   %xmm3
	mulsd	ALPHA_I, %xmm3
	mulsd	ALPHA_R, %xmm7

#ifndef XCONJ
	subsd	%xmm3,  %xmm6
	addsd	%xmm2,  %xmm7
#else
	addsd	%xmm3,  %xmm6
	subsd	%xmm2,  %xmm7
#endif

	movsd	 -16 * SIZE(Y1), %xmm0
	movsd	 -15 * SIZE(Y1), %xmm1
	ALIGN_3

	movl	M,   I
	sarl	$2,  I
	jle	.L15

	movsd	 -16 * SIZE(A1), %xmm2
	movsd	 -15 * SIZE(A1), %xmm3

	movapd	%xmm2, %xmm4
	mulsd	%xmm6, %xmm2
	mulsd	%xmm7, %xmm4

	decl	 I
	jle	 .L14
	ALIGN_3

.L13:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE + 0) * SIZE(A1)
#endif

	movapd	%xmm3, %xmm5
	mulsd	%xmm7, %xmm3
	ADD1	%xmm2, %xmm0
	movsd	 -14 * SIZE(A1), %xmm2
	mulsd	%xmm6, %xmm5
	ADD2	%xmm4, %xmm1

	movapd	%xmm2, %xmm4
	mulsd	%xmm6, %xmm2
	ADD3	%xmm3, %xmm0
	movsd	 -13 * SIZE(A1), %xmm3
	ADD4	%xmm5, %xmm1
	mulsd	%xmm7, %xmm4

	movlpd	 %xmm0, -16 * SIZE(Y1)
	movsd	 -14 * SIZE(Y1), %xmm0
	movlpd	 %xmm1, -15 * SIZE(Y1)
	movsd	 -13 * SIZE(Y1), %xmm1

	movapd	%xmm3, %xmm5
	mulsd	%xmm7, %xmm3
	ADD1	%xmm2, %xmm0
	movsd	 -12 * SIZE(A1), %xmm2
	mulsd	%xmm6, %xmm5
	ADD2	%xmm4, %xmm1

	movapd	%xmm2, %xmm4
	mulsd	%xmm6, %xmm2
	ADD3	%xmm3, %xmm0
	movsd	 -11 * SIZE(A1), %xmm3
	mulsd	%xmm7, %xmm4
	ADD4	%xmm5, %xmm1

	movlpd	 %xmm0, -14 * SIZE(Y1)
	movsd	 -12 * SIZE(Y1), %xmm0
	movlpd	 %xmm1, -13 * SIZE(Y1)
	movsd	 -11 * SIZE(Y1), %xmm1

	movapd	%xmm3, %xmm5
	mulsd	%xmm7, %xmm3
	ADD1	%xmm2, %xmm0
	movsd	 -10 * SIZE(A1), %xmm2
	mulsd	%xmm6, %xmm5
	ADD2	%xmm4, %xmm1

	movapd	%xmm2, %xmm4
	mulsd	%xmm6, %xmm2
	ADD3	%xmm3, %xmm0
	movsd	  -9 * SIZE(A1), %xmm3
	ADD4	%xmm5, %xmm1
	mulsd	%xmm7, %xmm4

	movlpd	 %xmm0, -12 * SIZE(Y1)
	movsd	 -10 * SIZE(Y1), %xmm0
	movlpd	 %xmm1, -11 * SIZE(Y1)
	movsd	  -9 * SIZE(Y1), %xmm1

	movapd	%xmm3, %xmm5
	mulsd	%xmm7, %xmm3
	ADD1	%xmm2, %xmm0
	movsd	  -8 * SIZE(A1), %xmm2
	mulsd	%xmm6, %xmm5
	ADD2	%xmm4, %xmm1

	movapd	%xmm2, %xmm4
	mulsd	%xmm6, %xmm2
	ADD3	%xmm3, %xmm0
	movsd	  -7 * SIZE(A1), %xmm3
	mulsd	%xmm7, %xmm4
	ADD4	%xmm5, %xmm1

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
	movapd	%xmm3, %xmm5
	mulsd	%xmm7, %xmm3
	ADD1	%xmm2, %xmm0
	movsd	 -14 * SIZE(A1), %xmm2
	mulsd	%xmm6, %xmm5
	ADD2	%xmm4, %xmm1

	movapd	%xmm2, %xmm4
	mulsd	%xmm6, %xmm2
	ADD3	%xmm3, %xmm0
	movsd	 -13 * SIZE(A1), %xmm3
	ADD4	%xmm5, %xmm1
	mulsd	%xmm7, %xmm4

	movlpd	 %xmm0, -16 * SIZE(Y1)
	movsd	 -14 * SIZE(Y1), %xmm0
	movlpd	 %xmm1, -15 * SIZE(Y1)
	movsd	 -13 * SIZE(Y1), %xmm1

	movapd	%xmm3, %xmm5
	mulsd	%xmm7, %xmm3
	ADD1	%xmm2, %xmm0
	movsd	 -12 * SIZE(A1), %xmm2
	mulsd	%xmm6, %xmm5
	ADD2	%xmm4, %xmm1

	movapd	%xmm2, %xmm4
	mulsd	%xmm6, %xmm2
	ADD3	%xmm3, %xmm0
	movsd	 -11 * SIZE(A1), %xmm3
	mulsd	%xmm7, %xmm4
	ADD4	%xmm5, %xmm1

	movlpd	 %xmm0, -14 * SIZE(Y1)
	movsd	 -12 * SIZE(Y1), %xmm0
	movlpd	 %xmm1, -13 * SIZE(Y1)
	movsd	 -11 * SIZE(Y1), %xmm1

	movapd	%xmm3, %xmm5
	mulsd	%xmm7, %xmm3
	ADD1	%xmm2, %xmm0
	movsd	 -10 * SIZE(A1), %xmm2
	mulsd	%xmm6, %xmm5
	ADD2	%xmm4, %xmm1

	movapd	%xmm2, %xmm4
	mulsd	%xmm6, %xmm2
	ADD3	%xmm3, %xmm0
	movsd	  -9 * SIZE(A1), %xmm3
	ADD4	%xmm5, %xmm1
	mulsd	%xmm7, %xmm4

	movlpd	 %xmm0, -12 * SIZE(Y1)
	movsd	 -10 * SIZE(Y1), %xmm0
	movlpd	 %xmm1, -11 * SIZE(Y1)
	movsd	  -9 * SIZE(Y1), %xmm1

	movapd	%xmm3, %xmm5
	mulsd	%xmm7, %xmm3
	ADD1	%xmm2, %xmm0
	mulsd	%xmm6, %xmm5
	ADD2	%xmm4, %xmm1

	ADD3	%xmm3, %xmm0
	ADD4	%xmm5, %xmm1

	movlpd	 %xmm0, -10 * SIZE(Y1)
	movsd	  -8 * SIZE(Y1), %xmm0
	movlpd	 %xmm1,  -9 * SIZE(Y1)
	movsd	  -7 * SIZE(Y1), %xmm1

	subl	 $-8 * SIZE, A1
	subl	 $-8 * SIZE, Y1
	ALIGN_3

.L15:
	testl	$2, M
	je	.L17

	movsd	 -16 * SIZE(A1), %xmm2
	movsd	 -15 * SIZE(A1), %xmm3

	movapd	%xmm2, %xmm4
	mulsd	%xmm6, %xmm2
	mulsd	%xmm7, %xmm4

	movapd	%xmm3, %xmm5
	mulsd	%xmm7, %xmm3
	ADD1	%xmm2, %xmm0
	movsd	 -14 * SIZE(A1), %xmm2
	mulsd	%xmm6, %xmm5
	ADD2	%xmm4, %xmm1

	movapd	%xmm2, %xmm4
	mulsd	%xmm6, %xmm2
	ADD3	%xmm3, %xmm0
	movsd	 -13 * SIZE(A1), %xmm3
	ADD4	%xmm5, %xmm1
	mulsd	%xmm7, %xmm4

	movlpd	 %xmm0, -16 * SIZE(Y1)
	movsd	 -14 * SIZE(Y1), %xmm0
	movlpd	 %xmm1, -15 * SIZE(Y1)
	movsd	 -13 * SIZE(Y1), %xmm1

	movapd	%xmm3, %xmm5
	mulsd	%xmm7, %xmm3
	ADD1	%xmm2, %xmm0
	mulsd	%xmm6, %xmm5
	ADD2	%xmm4, %xmm1

	ADD3	%xmm3, %xmm0
	ADD4	%xmm5, %xmm1

	movlpd	 %xmm0, -14 * SIZE(Y1)
	movsd	 -12 * SIZE(Y1), %xmm0
	movlpd	 %xmm1, -13 * SIZE(Y1)
	movsd	 -11 * SIZE(Y1), %xmm1

	addl	 $4 * SIZE, A1
	addl	 $4 * SIZE, Y1
	ALIGN_3

.L17:
	testl	$1, M
	je	.L19

	movsd	 -16 * SIZE(A1), %xmm2
	movsd	 -15 * SIZE(A1), %xmm3

	movapd	%xmm2, %xmm4
	mulsd	%xmm6, %xmm2
	mulsd	%xmm7, %xmm4

	movapd	%xmm3, %xmm5
	mulsd	%xmm7, %xmm3
	ADD1	%xmm2, %xmm0
	mulsd	%xmm6, %xmm5
	ADD2	%xmm4, %xmm1

	ADD3	%xmm3, %xmm0
	ADD4	%xmm5, %xmm1

	movlpd	 %xmm0, -16 * SIZE(Y1)
	movlpd	 %xmm1, -15 * SIZE(Y1)
	ALIGN_3

.L19:
	decl	J
	jg	.L10
	ALIGN_4

.L990:
	movl	Y,   Y1
	movl	BUFFER, X
	movl	STACK_INCY, INCY

	movl	Y1, A1
	sall	$ZBASE_SHIFT, INCY

	movl	M,   %eax
	sarl	$2,  %eax
	jle	.L994
	ALIGN_3

.L992:
	movsd	0 * SIZE(Y1), %xmm0
	movsd	1 * SIZE(Y1), %xmm1
	addl	INCY, Y1

	movsd	0 * SIZE(Y1), %xmm2
	movsd	1 * SIZE(Y1), %xmm3
	addl	INCY, Y1

	movsd	0 * SIZE(Y1), %xmm4
	movsd	1 * SIZE(Y1), %xmm5
	addl	INCY, Y1

	movsd	0 * SIZE(Y1), %xmm6
	movsd	1 * SIZE(Y1), %xmm7
	addl	INCY, Y1

	addsd	0 * SIZE(X), %xmm0
	addsd	1 * SIZE(X), %xmm1
	addsd	2 * SIZE(X), %xmm2
	addsd	3 * SIZE(X), %xmm3
	addsd	4 * SIZE(X), %xmm4
	addsd	5 * SIZE(X), %xmm5
	addsd	6 * SIZE(X), %xmm6
	addsd	7 * SIZE(X), %xmm7

	movlpd	%xmm0, 0 * SIZE(A1)
	movlpd	%xmm1, 1 * SIZE(A1)
	addl	INCY, A1

	movlpd	%xmm2, 0 * SIZE(A1)
	movlpd	%xmm3, 1 * SIZE(A1)
	addl	INCY, A1

	movlpd	%xmm4, 0 * SIZE(A1)
	movlpd	%xmm5, 1 * SIZE(A1)
	addl	INCY, A1

	movlpd	%xmm6, 0 * SIZE(A1)
	movlpd	%xmm7, 1 * SIZE(A1)
	addl	INCY, A1

	addl	$8 * SIZE, X
	decl	%eax
	jg	.L992
	ALIGN_3

.L994:
	testl	$2, M
	jle	.L996

	movsd	0 * SIZE(Y1), %xmm0
	movsd	1 * SIZE(Y1), %xmm1
	addl	INCY, Y1

	movsd	0 * SIZE(Y1), %xmm2
	movsd	1 * SIZE(Y1), %xmm3
	addl	INCY, Y1

	addsd	0 * SIZE(X), %xmm0
	addsd	1 * SIZE(X), %xmm1
	addsd	2 * SIZE(X), %xmm2
	addsd	3 * SIZE(X), %xmm3

	movlpd	%xmm0, 0 * SIZE(A1)
	movlpd	%xmm1, 1 * SIZE(A1)
	addl	INCY, A1

	movlpd	%xmm2, 0 * SIZE(A1)
	movlpd	%xmm3, 1 * SIZE(A1)
	addl	INCY, A1

	addl	$4 * SIZE, X
	ALIGN_3

.L996:
	testl	$1, M
	jle	.L999

	movsd	0 * SIZE(Y1), %xmm0
	movsd	1 * SIZE(Y1), %xmm1

	addsd	0 * SIZE(X), %xmm0
	addsd	1 * SIZE(X), %xmm1

	movlpd	%xmm0, 0 * SIZE(A1)
	movlpd	%xmm1, 1 * SIZE(A1)
	ALIGN_3

.L999:
	popl	%ebx
	popl	%esi
	popl	%edi
	popl	%ebp
	ret

	EPILOGUE
