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

#ifdef PENTIUM4
#define PREFETCH	prefetcht0
#define PREFETCHW	prefetcht0
#define PREFETCHSIZE	(8 * 2)
#endif

#if defined(CORE2) || defined(PENRYN) || defined(DUNNINGTON) || defined(NEHALEM) || defined(SANDYBRIDGE)
#define PREFETCH	prefetcht0
#define PREFETCHW	prefetcht0
#define PREFETCHSIZE	(8 * 7)
#endif

#ifdef OPTERON
#define PREFETCH	prefetchnta
#define PREFETCHW	prefetchw
#define PREFETCHSIZE	(8 * 3)
#define movsd		movlps
#endif

#if defined(BARCELONA) || defined(BULLDOZER)
#define PREFETCH	prefetchnta
#define PREFETCHW	prefetchw
#define PREFETCHSIZE	(8 * 5)
#endif

#ifdef ATOM
#define PREFETCH	prefetchnta
#define PREFETCHW	prefetcht0
#define PREFETCHSIZE	(8 * 6)
#endif

#ifdef NANO
#define PREFETCH	prefetcht0
#define PREFETCHSIZE	(8 * 4)
#endif

#define STACKSIZE	16
#define ARGS	16

#define M		 4 + STACKSIZE+ARGS(%esp)
#define N		 8 + STACKSIZE+ARGS(%esp)
#define ALPHA_R		16 + STACKSIZE+ARGS(%esp)
#define ALPHA_I		24 + STACKSIZE+ARGS(%esp)
#define A		32 + STACKSIZE+ARGS(%esp)
#define STACK_LDA	36 + STACKSIZE+ARGS(%esp)
#define STACK_X		40 + STACKSIZE+ARGS(%esp)
#define STACK_INCX	44 + STACKSIZE+ARGS(%esp)
#define Y		48 + STACKSIZE+ARGS(%esp)
#define STACK_INCY	52 + STACKSIZE+ARGS(%esp)
#define BUFFER		56 + STACKSIZE+ARGS(%esp)
#define MMM	0 + ARGS(%esp)
#define YY	4 + ARGS(%esp)
#define AA	8 + ARGS(%esp)


#define I	%eax
#define J	%ebx

#define INCX	%ecx
#define INCY	J

#define A1	%esi
#define X	%edx
#define Y1	%edi
#define LDA	%ebp

#undef SUBPD

#if (!defined(CONJ) && !defined(XCONJ)) || (defined(CONJ) && defined(XCONJ))
#define SUBPD	   subpd
#else
#define SUBPD	   addpd
#endif

	PROLOGUE

	subl	$ARGS,%esp
	pushl	%ebp
	pushl	%edi
	pushl	%esi
	pushl	%ebx

	PROFCODE

	movl	Y,J
	movl	J,YY
	movl	A,J
	movl	J,AA
	movl	M,J
	movl	J,MMM
.L0t:
	xorl	J,J
	addl	$1,J
	sall	$18,J
	subl	J,MMM
	movl	J,M
	jge	.L00t
	ALIGN_3

	movl	MMM,%eax
	addl	J,%eax
	jle	.L999x
	movl	%eax,M

.L00t:
	movl	AA,%eax
	movl	%eax,A

	movl	YY,J
	movl	J,Y

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
	movhpd	1 * SIZE(X), %xmm6
	addl	INCX, X

	pcmpeqb	%xmm5, %xmm5
	psllq	$63,    %xmm5
	shufps	$0xc0, %xmm5, %xmm5

	pshufd	$0x4e, %xmm6, %xmm7

#ifdef HAVE_SSE3
	movddup	ALPHA_R, %xmm3
	movddup	ALPHA_I, %xmm4
#else
	movsd	ALPHA_R, %xmm3
	movsd	ALPHA_I, %xmm4

	unpcklpd %xmm3, %xmm3
	unpcklpd %xmm4, %xmm4
#endif

	xorpd	 %xmm5, %xmm7

	mulpd	 %xmm3, %xmm6
	mulpd	 %xmm4, %xmm7

#ifndef XCONJ
	subpd	 %xmm7, %xmm6
#else
	addpd	 %xmm7, %xmm6
#endif

	pshufd	 $0xee, %xmm6, %xmm7
	pshufd	 $0x44, %xmm6, %xmm6

#ifndef CONJ
	xorpd	 %xmm5, %xmm7
#else
	xorpd	 %xmm5, %xmm6
#endif

	movapd	 -16 * SIZE(Y1), %xmm0
	movapd	 -14 * SIZE(Y1), %xmm1
	ALIGN_3

	movl	M,   I
	sarl	$2,  I
	jle	.L15

	movsd	 -16 * SIZE(A1), %xmm2
	movhpd	 -15 * SIZE(A1), %xmm2
	movsd	 -14 * SIZE(A1), %xmm4
	movhpd	 -13 * SIZE(A1), %xmm4

	decl	 I
	jle	 .L14
	ALIGN_3

.L13:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE + 0) * SIZE(A1)
#endif

	pshufd	 $0x4e, %xmm2,  %xmm3
	mulpd	 %xmm6, %xmm2
	addpd	 %xmm2, %xmm0
	movsd	 -12 * SIZE(A1), %xmm2
	movhpd	 -11 * SIZE(A1), %xmm2
	pshufd	 $0x4e, %xmm4,  %xmm5
	mulpd	 %xmm6, %xmm4
	addpd	 %xmm4, %xmm1
	movsd	 -10 * SIZE(A1), %xmm4
	movhpd	  -9 * SIZE(A1), %xmm4

	mulpd	 %xmm7, %xmm3
	SUBPD	 %xmm3, %xmm0
	movapd	 %xmm0, -16 * SIZE(Y1)
	movapd	 -12 * SIZE(Y1), %xmm0
	mulpd	 %xmm7, %xmm5
	SUBPD	 %xmm5, %xmm1
	movapd	 %xmm1, -14 * SIZE(Y1)
	movapd	 -10 * SIZE(Y1), %xmm1

	pshufd	 $0x4e, %xmm2,  %xmm3
	mulpd	 %xmm6, %xmm2
	addpd	 %xmm2, %xmm0
	movsd	  -8 * SIZE(A1), %xmm2
	movhpd	  -7 * SIZE(A1), %xmm2
	pshufd	 $0x4e, %xmm4,  %xmm5
	mulpd	 %xmm6, %xmm4
	addpd	 %xmm4, %xmm1
	movsd	  -6 * SIZE(A1), %xmm4
	movhpd	  -5 * SIZE(A1), %xmm4

	mulpd	 %xmm7, %xmm3
	SUBPD	 %xmm3, %xmm0
	movapd	 %xmm0, -12 * SIZE(Y1)
	movapd	  -8 * SIZE(Y1), %xmm0
	mulpd	 %xmm7, %xmm5
	SUBPD	 %xmm5, %xmm1
	movapd	 %xmm1, -10 * SIZE(Y1)
	movapd	  -6 * SIZE(Y1), %xmm1

	subl	 $-8 * SIZE, A1
	subl	 $-8 * SIZE, Y1

	subl	 $1, I
	BRANCH
	jg	.L13
	ALIGN_3

.L14:
	pshufd	 $0x4e, %xmm2,  %xmm3
	mulpd	 %xmm6, %xmm2
	addpd	 %xmm2, %xmm0
	movsd	 -12 * SIZE(A1), %xmm2
	movhpd	 -11 * SIZE(A1), %xmm2
	pshufd	 $0x4e, %xmm4,  %xmm5
	mulpd	 %xmm6, %xmm4
	addpd	 %xmm4, %xmm1
	movsd	 -10 * SIZE(A1), %xmm4
	movhpd	  -9 * SIZE(A1), %xmm4

	mulpd	 %xmm7, %xmm3
	SUBPD	 %xmm3, %xmm0
	movapd	 %xmm0, -16 * SIZE(Y1)
	movapd	 -12 * SIZE(Y1), %xmm0
	mulpd	 %xmm7, %xmm5
	SUBPD	 %xmm5, %xmm1
	movapd	 %xmm1, -14 * SIZE(Y1)
	movapd	 -10 * SIZE(Y1), %xmm1

	pshufd	 $0x4e, %xmm2,  %xmm3
	mulpd	 %xmm6, %xmm2
	addpd	 %xmm2, %xmm0
	pshufd	 $0x4e, %xmm4,  %xmm5
	mulpd	 %xmm6, %xmm4
	addpd	 %xmm4, %xmm1

	mulpd	 %xmm7, %xmm3
	SUBPD	 %xmm3, %xmm0
	movapd	 %xmm0, -12 * SIZE(Y1)
	movapd	  -8 * SIZE(Y1), %xmm0
	mulpd	 %xmm7, %xmm5
	SUBPD	 %xmm5, %xmm1
	movapd	 %xmm1, -10 * SIZE(Y1)
	movapd	  -6 * SIZE(Y1), %xmm1

	subl	 $-8 * SIZE, A1
	subl	 $-8 * SIZE, Y1
	ALIGN_3

.L15:
	testl	$2, M
	je	.L17

	movsd	 -16 * SIZE(A1), %xmm2
	movhpd	 -15 * SIZE(A1), %xmm2
	movsd	 -14 * SIZE(A1), %xmm4
	movhpd	 -13 * SIZE(A1), %xmm4

	pshufd	 $0x4e, %xmm2,  %xmm3
	mulpd	 %xmm6, %xmm2
	addpd	 %xmm2, %xmm0
	pshufd	 $0x4e, %xmm4,  %xmm5
	mulpd	 %xmm6, %xmm4
	addpd	 %xmm4, %xmm1

	mulpd	 %xmm7, %xmm3
	SUBPD	 %xmm3, %xmm0
	movapd	 %xmm0, -16 * SIZE(Y1)
	mulpd	 %xmm7, %xmm5
	SUBPD	 %xmm5, %xmm1
	movapd	 %xmm1, -14 * SIZE(Y1)

	movapd	 -12 * SIZE(Y1), %xmm0

	addl	 $4 * SIZE, A1
	addl	 $4 * SIZE, Y1
	ALIGN_3

.L17:
	testl	$1, M
	je	.L19

	movsd	 -16 * SIZE(A1), %xmm2
	movhpd	 -15 * SIZE(A1), %xmm2

	pshufd	 $0x4e, %xmm2, %xmm3
	mulpd	 %xmm6, %xmm2
	addpd	 %xmm2, %xmm0
	mulpd	 %xmm7, %xmm3
	SUBPD	 %xmm3, %xmm0

	movapd	 %xmm0, -16 * SIZE(Y1)
	ALIGN_3

.L19:
	decl	J
	jg	.L10
	ALIGN_4

.L990:
	movl	Y,   Y1
	movl	BUFFER, X

	movl	STACK_INCY, INCY
	sall	$ZBASE_SHIFT, INCY

	movl	M,   %eax
	sarl	$2,  %eax
	jle	.L994
	ALIGN_3

.L992:
	movsd	0 * SIZE(Y1), %xmm0
	movhpd	1 * SIZE(Y1), %xmm0

	addpd	0 * SIZE(X), %xmm0

	movlpd	%xmm0, 0 * SIZE(Y1)
	movhpd	%xmm0, 1 * SIZE(Y1)
	addl	INCY, Y1

	movsd	0 * SIZE(Y1), %xmm0
	movhpd	1 * SIZE(Y1), %xmm0

	addpd	2 * SIZE(X), %xmm0

	movlpd	%xmm0, 0 * SIZE(Y1)
	movhpd	%xmm0, 1 * SIZE(Y1)
	addl	INCY, Y1

	movsd	0 * SIZE(Y1), %xmm0
	movhpd	1 * SIZE(Y1), %xmm0

	addpd	4 * SIZE(X), %xmm0

	movlpd	%xmm0, 0 * SIZE(Y1)
	movhpd	%xmm0, 1 * SIZE(Y1)
	addl	INCY, Y1

	movsd	0 * SIZE(Y1), %xmm0
	movhpd	1 * SIZE(Y1), %xmm0

	addpd	6 * SIZE(X), %xmm0

	movlpd	%xmm0, 0 * SIZE(Y1)
	movhpd	%xmm0, 1 * SIZE(Y1)
	addl	INCY, Y1

	addl	$8 * SIZE, X
	decl	%eax
	jg	.L992
	ALIGN_3

.L994:
	testl	$2, M
	jle	.L996

	movsd	0 * SIZE(Y1), %xmm0
	movhpd	1 * SIZE(Y1), %xmm0

	addpd	0 * SIZE(X), %xmm0

	movlpd	%xmm0, 0 * SIZE(Y1)
	movhpd	%xmm0, 1 * SIZE(Y1)
	addl	INCY, Y1

	movsd	0 * SIZE(Y1), %xmm0
	movhpd	1 * SIZE(Y1), %xmm0

	addpd	2 * SIZE(X), %xmm0

	movlpd	%xmm0, 0 * SIZE(Y1)
	movhpd	%xmm0, 1 * SIZE(Y1)
	addl	INCY, Y1

	addl	$4 * SIZE, X
	ALIGN_3

.L996:
	testl	$1, M
	jle	.L999

	movsd	0 * SIZE(Y1), %xmm0
	movhpd	1 * SIZE(Y1), %xmm0

	addpd	0 * SIZE(X), %xmm0

	movlpd	%xmm0, 0 * SIZE(Y1)
	movhpd	%xmm0, 1 * SIZE(Y1)
	ALIGN_3

.L999:
	movl	M,%eax
	sall	$ZBASE_SHIFT,%eax
	addl	%eax,AA
	movl	STACK_INCY,INCY
	imull	INCY,%eax
	addl	%eax,YY
	jmp		.L0t
	ALIGN_3

.L999x:
	popl	%ebx
	popl	%esi
	popl	%edi
	popl	%ebp
	addl	$ARGS,%esp
	ret

	EPILOGUE
