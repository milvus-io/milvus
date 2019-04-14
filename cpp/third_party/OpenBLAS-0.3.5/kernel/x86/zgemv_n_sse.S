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

#ifdef movsd
#undef movsd
#endif

#ifdef PENTIUM3
#ifdef HAVE_SSE
#define PREFETCH	prefetcht0
#define PREFETCHW	prefetcht0
#define PREFETCHSIZE	(16 * 2)
#endif
#define movsd		movlps
#endif

#ifdef PENTIUM4
#define PREFETCH	prefetcht0
#define PREFETCHW	prefetcht0
#define PREFETCHSIZE	(16 * 2)
#endif

#if defined(CORE2) || defined(PENRYN) || defined(DUNNINGTON) || defined(NEHALEM) || defined(SANDYBRIDGE)
#define PREFETCH	prefetcht0
#define PREFETCHW	prefetcht0
#define PREFETCHSIZE	(16 * 7)
#endif

#ifdef OPTERON
#define PREFETCH	prefetchnta
#define PREFETCHW	prefetchw
#define PREFETCHSIZE	(16 * 3)
#define movsd		movlps
#endif

#if defined(BARCELONA)  || defined(BULLDOZER)
#define PREFETCH	prefetchnta
#define PREFETCHW	prefetchw
#define PREFETCHSIZE	(16 * 5)
#endif

#ifdef ATOM
#define PREFETCH	prefetchnta
#define PREFETCHW	prefetcht0
#define PREFETCHSIZE	(16 * 6)
#endif

#ifdef NANO
#define PREFETCH	prefetcht0
#define PREFETCHSIZE	(16 * 4)
#endif

#define STACKSIZE	16
#define ARGS	20

#define M		 4 + STACKSIZE+ARGS(%esp)
#define N		 8 + STACKSIZE+ARGS(%esp)
#define ALPHA_R		16 + STACKSIZE+ARGS(%esp)
#define ALPHA_I		20 + STACKSIZE+ARGS(%esp)
#define A		24 + STACKSIZE+ARGS(%esp)
#define STACK_LDA	28 + STACKSIZE+ARGS(%esp)
#define STACK_X		32 + STACKSIZE+ARGS(%esp)
#define STACK_INCX	36 + STACKSIZE+ARGS(%esp)
#define Y		40 + STACKSIZE+ARGS(%esp)
#define STACK_INCY	44 + STACKSIZE+ARGS(%esp)
#define BUFFER		48 + STACKSIZE+ARGS(%esp)

#define MMM	0+ARGS(%esp)
#define YY	4+ARGS(%esp)
#define AA	8+ARGS(%esp)

#define I	%eax
#define J	%ebx

#define INCX	%ecx
#define INCY	J

#define A1	%esi
#define X	%edx
#define Y1	%edi
#define LDA	%ebp

#undef SUBPS

#if (!defined(CONJ) && !defined(XCONJ)) || (defined(CONJ) && defined(XCONJ))
#define SUBPS	   subps
#else
#define SUBPS	   addps
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
	sall	$20,J
	subl	J,MMM
	movl	J,M
	jge		.L00t
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

	subl	$-32 * SIZE, A

	cmpl	$0, N
	jle	.L999
	cmpl	$0, M
	jle	.L999

	movl	BUFFER, Y1

	movl	N,  J

	xorps	%xmm7, %xmm7

	movl	M,  %eax
	addl	$8, %eax
	sarl	$3, %eax
	ALIGN_3

.L01:
	movaps	%xmm7,  0 * SIZE(Y1)
	movaps	%xmm7,  4 * SIZE(Y1)
	movaps	%xmm7,  8 * SIZE(Y1)
	movaps	%xmm7, 12 * SIZE(Y1)
	subl	$-16 * SIZE, Y1
	decl	%eax
	jg	.L01
	ALIGN_3

.L10:
	movl	BUFFER, Y1
	addl	$32 * SIZE, Y1

	movl	A,  A1
	addl	LDA, A

	movsd	(X), %xmm7
	addl	INCX, X

#ifdef HAVE_SSE2
	pcmpeqb	%xmm5, %xmm5
	psllq	$63,   %xmm5
#else
	subl	$8, %esp
	movl	$0x00000000,  0(%esp)
	movl	$0x80000000,  4(%esp)
	movlps	(%esp), %xmm5
	addl	$8, %esp
	movlhps	%xmm5, %xmm5
#endif

#ifdef HAVE_SSE2
	pshufd	$0x00, %xmm7, %xmm6
	pshufd	$0x55, %xmm7, %xmm7
#else
	movaps	%xmm7, %xmm6
	shufps	$0x00, %xmm6, %xmm6
	shufps	$0x55, %xmm7, %xmm7
#endif

#ifdef HAVE_SSE3
	movddup	ALPHA_R, %xmm3
#else
	movsd	ALPHA_R, %xmm3

	movlhps %xmm3, %xmm3
#endif

#ifdef HAVE_SSE2
	pshufd	$0xb1, %xmm3, %xmm4
#else
	movaps	%xmm3, %xmm4
	shufps	$0xb1, %xmm4, %xmm4
#endif


#ifndef XCONJ
	xorps	 %xmm5, %xmm7
#else
	xorps	 %xmm5, %xmm6
#endif

	mulps	 %xmm3, %xmm6
	mulps	 %xmm4, %xmm7

#ifndef XCONJ
	subps	 %xmm7, %xmm6
#else
	addps	 %xmm7, %xmm6
#endif

#ifdef HAVE_SSE2
	pshufd	 $0x55, %xmm6, %xmm7
	pshufd	 $0x00, %xmm6, %xmm6
#else
	movaps	 %xmm6, %xmm7
	shufps	 $0x55, %xmm7, %xmm7
	shufps	 $0x00, %xmm6, %xmm6
#endif

#ifndef CONJ
	xorps	 %xmm5, %xmm7
#else
	xorps	 %xmm5, %xmm6
#endif

	movaps	 -32 * SIZE(Y1), %xmm0
	movaps	 -28 * SIZE(Y1), %xmm1
	ALIGN_3

	movl	M,   I
	sarl	$3,  I
	jle	.L15

	movsd	 -32 * SIZE(A1), %xmm2
	movhps	 -30 * SIZE(A1), %xmm2
	movsd	 -28 * SIZE(A1), %xmm4
	movhps	 -26 * SIZE(A1), %xmm4

	decl	 I
	jle	 .L14
	ALIGN_3

.L13:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE + 0) * SIZE(A1)
#endif

#ifdef HAVE_SSE2
	pshufd	 $0xb1, %xmm2,  %xmm3
#else
	movaps	 %xmm2, %xmm3
	shufps	 $0xb1, %xmm3,  %xmm3
#endif
	mulps	 %xmm6, %xmm2
	addps	 %xmm2, %xmm0
	movsd	 -24 * SIZE(A1), %xmm2
	movhps	 -22 * SIZE(A1), %xmm2
#ifdef HAVE_SSE2
	pshufd	 $0xb1, %xmm4,  %xmm5
#else
	movaps	 %xmm4, %xmm5
	shufps	 $0xb1, %xmm5,  %xmm5
#endif
	mulps	 %xmm6, %xmm4
	addps	 %xmm4, %xmm1
	movsd	 -20 * SIZE(A1), %xmm4
	movhps	 -18 * SIZE(A1), %xmm4

	mulps	 %xmm7, %xmm3
	SUBPS	 %xmm3, %xmm0
	movaps	 %xmm0, -32 * SIZE(Y1)
	movaps	 -24 * SIZE(Y1), %xmm0
	mulps	 %xmm7, %xmm5
	SUBPS	 %xmm5, %xmm1
	movaps	 %xmm1, -28 * SIZE(Y1)
	movaps	 -20 * SIZE(Y1), %xmm1

#ifdef HAVE_SSE2
	pshufd	 $0xb1, %xmm2,  %xmm3
#else
	movaps	 %xmm2, %xmm3
	shufps	 $0xb1, %xmm3,  %xmm3
#endif
	mulps	 %xmm6, %xmm2
	addps	 %xmm2, %xmm0
	movsd	 -16 * SIZE(A1), %xmm2
	movhps	 -14 * SIZE(A1), %xmm2
#ifdef HAVE_SSE2
	pshufd	 $0xb1, %xmm4,  %xmm5
#else
	movaps	 %xmm4, %xmm5
	shufps	 $0xb1, %xmm5,  %xmm5
#endif
	mulps	 %xmm6, %xmm4
	addps	 %xmm4, %xmm1
	movsd	 -12 * SIZE(A1), %xmm4
	movhps	 -10 * SIZE(A1), %xmm4

	mulps	 %xmm7, %xmm3
	SUBPS	 %xmm3, %xmm0
	movaps	 %xmm0, -24 * SIZE(Y1)
	movaps	 -16 * SIZE(Y1), %xmm0
	mulps	 %xmm7, %xmm5
	SUBPS	 %xmm5, %xmm1
	movaps	 %xmm1, -20 * SIZE(Y1)
	movaps	 -12 * SIZE(Y1), %xmm1

	subl	 $-16 * SIZE, A1
	subl	 $-16 * SIZE, Y1

	subl	 $1, I
	BRANCH
	jg	.L13
	ALIGN_3

.L14:
#ifdef HAVE_SSE2
	pshufd	 $0xb1, %xmm2,  %xmm3
#else
	movaps	 %xmm2, %xmm3
	shufps	 $0xb1, %xmm3,  %xmm3
#endif
	mulps	 %xmm6, %xmm2
	addps	 %xmm2, %xmm0
	movsd	 -24 * SIZE(A1), %xmm2
	movhps	 -22 * SIZE(A1), %xmm2
#ifdef HAVE_SSE2
	pshufd	 $0xb1, %xmm4,  %xmm5
#else
	movaps	 %xmm4, %xmm5
	shufps	 $0xb1, %xmm5,  %xmm5
#endif
	mulps	 %xmm6, %xmm4
	addps	 %xmm4, %xmm1
	movsd	 -20 * SIZE(A1), %xmm4
	movhps	 -18 * SIZE(A1), %xmm4

	mulps	 %xmm7, %xmm3
	SUBPS	 %xmm3, %xmm0
	movaps	 %xmm0, -32 * SIZE(Y1)
	movaps	 -24 * SIZE(Y1), %xmm0
	mulps	 %xmm7, %xmm5
	SUBPS	 %xmm5, %xmm1
	movaps	 %xmm1, -28 * SIZE(Y1)
	movaps	 -20 * SIZE(Y1), %xmm1

#ifdef HAVE_SSE2
	pshufd	 $0xb1, %xmm2,  %xmm3
#else
	movaps	 %xmm2, %xmm3
	shufps	 $0xb1, %xmm3,  %xmm3
#endif
	mulps	 %xmm6, %xmm2
	addps	 %xmm2, %xmm0
#ifdef HAVE_SSE2
	pshufd	 $0xb1, %xmm4,  %xmm5
#else
	movaps	 %xmm4, %xmm5
	shufps	 $0xb1, %xmm5,  %xmm5
#endif
	mulps	 %xmm6, %xmm4
	addps	 %xmm4, %xmm1

	mulps	 %xmm7, %xmm3
	SUBPS	 %xmm3, %xmm0
	movaps	 %xmm0, -24 * SIZE(Y1)
	movaps	 -16 * SIZE(Y1), %xmm0
	mulps	 %xmm7, %xmm5
	SUBPS	 %xmm5, %xmm1
	movaps	 %xmm1, -20 * SIZE(Y1)
	movaps	 -12 * SIZE(Y1), %xmm1

	subl	 $-16 * SIZE, A1
	subl	 $-16 * SIZE, Y1
	ALIGN_3

.L15:
	testl	$4, M
	je	.L17

	movsd	 -32 * SIZE(A1), %xmm2
	movhps	 -30 * SIZE(A1), %xmm2
	movsd	 -28 * SIZE(A1), %xmm4
	movhps	 -26 * SIZE(A1), %xmm4

#ifdef HAVE_SSE2
	pshufd	 $0xb1, %xmm2,  %xmm3
#else
	movaps	 %xmm2, %xmm3
	shufps	 $0xb1, %xmm3,  %xmm3
#endif
	mulps	 %xmm6, %xmm2
	addps	 %xmm2, %xmm0
#ifdef HAVE_SSE2
	pshufd	 $0xb1, %xmm4,  %xmm5
#else
	movaps	 %xmm4, %xmm5
	shufps	 $0xb1, %xmm5,  %xmm5
#endif
	mulps	 %xmm6, %xmm4
	addps	 %xmm4, %xmm1

	mulps	 %xmm7, %xmm3
	SUBPS	 %xmm3, %xmm0
	movaps	 %xmm0, -32 * SIZE(Y1)
	movaps	 -24 * SIZE(Y1), %xmm0
	mulps	 %xmm7, %xmm5
	SUBPS	 %xmm5, %xmm1
	movaps	 %xmm1, -28 * SIZE(Y1)
	movaps	 -20 * SIZE(Y1), %xmm1

	addl	 $8 * SIZE, A1
	addl	 $8 * SIZE, Y1
	ALIGN_3

.L17:
	testl	$2, M
	je	.L18

	movsd	 -32 * SIZE(A1), %xmm2
	movhps	 -30 * SIZE(A1), %xmm2

#ifdef HAVE_SSE2
	pshufd	 $0xb1, %xmm2,  %xmm3
#else
	movaps	 %xmm2, %xmm3
	shufps	 $0xb1, %xmm3,  %xmm3
#endif
	mulps	 %xmm6, %xmm2
	addps	 %xmm2, %xmm0
	mulps	 %xmm7, %xmm3
	SUBPS	 %xmm3, %xmm0

	movaps	 %xmm0, -32 * SIZE(Y1)
	movaps	 %xmm1, %xmm0

	addl	 $4 * SIZE, A1
	addl	 $4 * SIZE, Y1
	ALIGN_3

.L18:
	testl	$1, M
	je	.L19

#ifdef	movsd
	xorps	%xmm2, %xmm2
#endif
	movsd	 -32 * SIZE(A1), %xmm2

#ifdef HAVE_SSE2
	pshufd	 $0xb1, %xmm2,  %xmm3
#else
	movaps	 %xmm2, %xmm3
	shufps	 $0xb1, %xmm3,  %xmm3
#endif
	mulps	 %xmm6, %xmm2
	addps	 %xmm2, %xmm0
	mulps	 %xmm7, %xmm3
	SUBPS	 %xmm3, %xmm0

	movlps	 %xmm0, -32 * SIZE(Y1)
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
	sarl	$3,  %eax
	jle	.L994
	ALIGN_3

.L992:
	movsd	(Y1), %xmm0
	movhps	(Y1, INCY), %xmm0

	addps	 0 * SIZE(X), %xmm0

	movlps	%xmm0, (Y1)
	movhps	%xmm0, (Y1, INCY)
	leal	(Y1, INCY, 2), Y1

	movsd	(Y1), %xmm0
	movhps	(Y1, INCY), %xmm0

	addps	 4 * SIZE(X), %xmm0

	movlps	%xmm0, (Y1)
	movhps	%xmm0, (Y1, INCY)
	leal	(Y1, INCY, 2), Y1

	movsd	(Y1), %xmm0
	movhps	(Y1, INCY), %xmm0

	addps	 8 * SIZE(X), %xmm0

	movlps	%xmm0, (Y1)
	movhps	%xmm0, (Y1, INCY)
	leal	(Y1, INCY, 2), Y1

	movsd	(Y1), %xmm0
	movhps	(Y1, INCY), %xmm0

	addps	12 * SIZE(X), %xmm0

	movlps	%xmm0, (Y1)
	movhps	%xmm0, (Y1, INCY)
	leal	(Y1, INCY, 2), Y1

	addl	$16 * SIZE, X
	decl	%eax
	jg	.L992
	ALIGN_3

.L994:
	testl	$4, M
	jle	.L995

	movsd	(Y1), %xmm0
	movhps	(Y1, INCY), %xmm0

	addps	 0 * SIZE(X), %xmm0

	movlps	%xmm0, (Y1)
	movhps	%xmm0, (Y1, INCY)
	leal	(Y1, INCY, 2), Y1

	movsd	(Y1), %xmm0
	movhps	(Y1, INCY), %xmm0

	addps	 4 * SIZE(X), %xmm0

	movlps	%xmm0, (Y1)
	movhps	%xmm0, (Y1, INCY)
	leal	(Y1, INCY, 2), Y1

	addl	$8 * SIZE, X
	ALIGN_3

.L995:
	testl	$2, M
	jle	.L996

	movsd	(Y1), %xmm0
	movhps	(Y1, INCY), %xmm0

	addps	 0 * SIZE(X), %xmm0

	movlps	%xmm0, (Y1)
	movhps	%xmm0, (Y1, INCY)
	leal	(Y1, INCY, 2), Y1

	addl	$4 * SIZE, X
	ALIGN_3

.L996:
	testl	$1, M
	jle	.L999

#ifdef	movsd
	xorps	%xmm0, %xmm0
#endif
	movsd	(Y1), %xmm0

	addps	 0 * SIZE(X), %xmm0

	movlps	%xmm0, (Y1)
	ALIGN_3

.L999:
	movl	M,%eax
	sall	$ZBASE_SHIFT,%eax
	addl	%eax,AA
	movl	STACK_INCY,INCY
	imull	INCY,%eax
	addl	%eax,YY
	jmp 	.L0t
	ALIGN_3

.L999x:
	popl	%ebx
	popl	%esi
	popl	%edi
	popl	%ebp
	addl	$ARGS,%esp
	ret

	EPILOGUE
