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
#define ARGS	20

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
#define AA	4 + ARGS(%esp)
#define XX	8 + ARGS(%esp)

#define I	%eax
#define J	%ebx

#define INCX	J
#define INCY	%ecx

#define A1	%esi
#define X	%edx
#define Y1	%edi
#define LDA	%ebp

#undef SUBPD

#ifndef CONJ
#define SUBPD	   addpd
#else
#define SUBPD	   subpd
#endif

	PROLOGUE

	subl	$ARGS,%esp
	pushl	%ebp
	pushl	%edi
	pushl	%esi
	pushl	%ebx

	PROFCODE

	movl	STACK_X, X
	movl	X, XX
	movl	A,J
	movl	J,AA
	movl	M,J
	movl	J,MMM
.L0t:
	xorl	J,J
	addl	$1,J
	sall	$18,J
	subl	$4,J
	subl	J,MMM
	movl	J,M
	jge	.L00t
	ALIGN_4

	movl	MMM,%eax
	addl	J,%eax
	jle	.L999x
	movl	%eax, M

.L00t:
	movl	XX, %eax
	movl	%eax, X

	movl	AA,%eax
	movl	%eax,A

	movl	STACK_LDA,  LDA
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
	ALIGN_4

.L11:
	movl	BUFFER, X
	addl	$16 * SIZE, X

	movl	A, A1
	addl	LDA, A

	xorps	%xmm0, %xmm0
	xorps	%xmm1, %xmm1

	movapd	-16 * SIZE(X), %xmm2
	movapd	-14 * SIZE(X), %xmm3

	movl	M,   I
	sarl	$2,  I
	jle	.L15

	movsd	-16 * SIZE(A1), %xmm4
	movhpd	-15 * SIZE(A1), %xmm4
	movsd	-14 * SIZE(A1), %xmm6
	movhpd	-13 * SIZE(A1), %xmm6

	decl	I
	jle	.L13
	ALIGN_4

.L12:
#ifdef PREFETCH
	PREFETCH PREFETCHSIZE * SIZE(A1)
#endif

	pshufd	$0x4e, %xmm4,  %xmm5
	mulpd	%xmm2,  %xmm4
	addpd	%xmm4,  %xmm0
	movsd	-12 * SIZE(A1), %xmm4
	movhpd	-11 * SIZE(A1), %xmm4
	mulpd	%xmm2,  %xmm5
	SUBPD	%xmm5,  %xmm1
	movapd	-12 * SIZE(X), %xmm2

	pshufd	$0x4e, %xmm6,  %xmm7
	mulpd	%xmm3,  %xmm6
	addpd	%xmm6,  %xmm0
	movsd	-10 * SIZE(A1), %xmm6
	movhpd	 -9 * SIZE(A1), %xmm6
	mulpd	%xmm3,  %xmm7
	SUBPD	%xmm7,  %xmm1
	movapd	-10 * SIZE(X), %xmm3

	pshufd	$0x4e, %xmm4,  %xmm5
	mulpd	%xmm2,  %xmm4
	addpd	%xmm4,  %xmm0
	movsd	 -8 * SIZE(A1), %xmm4
	movhpd	 -7 * SIZE(A1), %xmm4
	mulpd	%xmm2,  %xmm5
	movapd	 -8 * SIZE(X), %xmm2
	SUBPD	%xmm5,  %xmm1

	pshufd	$0x4e, %xmm6,  %xmm7
	mulpd	%xmm3,  %xmm6
	addpd	%xmm6,  %xmm0
	movsd	 -6 * SIZE(A1), %xmm6
	movhpd	 -5 * SIZE(A1), %xmm6
	mulpd	%xmm3,  %xmm7
	movapd	 -6 * SIZE(X), %xmm3
	SUBPD	%xmm7,  %xmm1

	addl	$8 * SIZE, A1
	addl	$8 * SIZE, X

	decl	I
	jg	.L12
	ALIGN_4

.L13:
	pshufd	$0x4e, %xmm4,  %xmm5
	mulpd	%xmm2,  %xmm4
	addpd	%xmm4,  %xmm0
	movsd	-12 * SIZE(A1), %xmm4
	movhpd	-11 * SIZE(A1), %xmm4
	mulpd	%xmm2,  %xmm5
	SUBPD	%xmm5,  %xmm1
	movapd	-12 * SIZE(X), %xmm2

	pshufd	$0x4e, %xmm6,  %xmm7
	mulpd	%xmm3,  %xmm6
	addpd	%xmm6,  %xmm0
	movsd	-10 * SIZE(A1), %xmm6
	movhpd	 -9 * SIZE(A1), %xmm6
	mulpd	%xmm3,  %xmm7
	SUBPD	%xmm7,  %xmm1
	movapd	-10 * SIZE(X), %xmm3

	pshufd	$0x4e, %xmm4,  %xmm5
	mulpd	%xmm2,  %xmm4
	addpd	%xmm4,  %xmm0
	mulpd	%xmm2,  %xmm5
	movapd	 -8 * SIZE(X), %xmm2
	SUBPD	%xmm5,  %xmm1

	pshufd	$0x4e, %xmm6,  %xmm7
	mulpd	%xmm3,  %xmm6
	addpd	%xmm6,  %xmm0
	mulpd	%xmm3,  %xmm7
	movapd	 -6 * SIZE(X), %xmm3
	SUBPD	%xmm7,  %xmm1

	addl	$8 * SIZE, A1
	addl	$8 * SIZE, X
	ALIGN_4

.L15:
	testl	$2, M
	jle	.L17

	movsd	-16 * SIZE(A1), %xmm4
	movhpd	-15 * SIZE(A1), %xmm4
	movsd	-14 * SIZE(A1), %xmm6
	movhpd	-13 * SIZE(A1), %xmm6

	pshufd	$0x4e, %xmm4,  %xmm5
	mulpd	%xmm2,  %xmm4
	addpd	%xmm4,  %xmm0
	mulpd	%xmm2,  %xmm5
	movapd	-12 * SIZE(X), %xmm2
	SUBPD	%xmm5,  %xmm1

	pshufd	$0x4e, %xmm6,  %xmm7
	mulpd	%xmm3,  %xmm6
	addpd	%xmm6,  %xmm0
	mulpd	%xmm3,  %xmm7
	SUBPD	%xmm7,  %xmm1

	addl	$4 * SIZE, A1
	ALIGN_4

.L17:
	testl	$1, M
	jle	.L18

	movsd	-16 * SIZE(A1), %xmm4
	movhpd	-15 * SIZE(A1), %xmm4

	pshufd	$0x4e, %xmm4,  %xmm5
	mulpd	%xmm2,  %xmm4
	addpd	%xmm4,  %xmm0
	mulpd	%xmm2,  %xmm5
	SUBPD	%xmm5,  %xmm1
	ALIGN_4

.L18:
	pcmpeqb	%xmm5, %xmm5
	psllq	$63,    %xmm5
	shufps	$0xc0, %xmm5, %xmm5

#if (!defined(CONJ) && !defined(XCONJ)) || (defined(CONJ) && defined(XCONJ))
	xorpd	%xmm5, %xmm0
#else
	xorpd	%xmm5, %xmm1
#endif

#ifdef HAVE_SSE3
	haddpd	%xmm1, %xmm0
#else
	movapd	%xmm0, %xmm2
	unpcklpd %xmm1, %xmm0
	unpckhpd %xmm1, %xmm2

	addpd	%xmm2, %xmm0
#endif

	pshufd	$0x4e, %xmm0, %xmm1

#ifdef HAVE_SSE3
	movddup	ALPHA_R, %xmm6
	movddup	ALPHA_I, %xmm7
#else
	movsd	ALPHA_R, %xmm6
	movsd	ALPHA_I, %xmm7

	unpcklpd %xmm6, %xmm6
	unpcklpd %xmm7, %xmm7
#endif

	mulpd	%xmm6, %xmm0
	mulpd	%xmm7, %xmm1

	xorpd	%xmm5, %xmm1

	subpd	%xmm1, %xmm0

	movsd	 0 * SIZE(Y1), %xmm4
	movhpd	 1 * SIZE(Y1), %xmm4

	addpd	%xmm4, %xmm0

	movlpd	%xmm0,  0 * SIZE(Y1)
	movhpd	%xmm0,  1 * SIZE(Y1)
	addl	INCY, Y1

	decl	J
	jg	.L11
	ALIGN_4

.L999:
	movl	M,%eax
	sall	$ZBASE_SHIFT,%eax
	addl	%eax,AA
	movl	STACK_INCX,INCX
	imull	INCX,%eax
	addl	%eax,XX
	jmp	.L0t
	ALIGN_4

.L999x:
	popl	%ebx
	popl	%esi
	popl	%edi
	popl	%ebp
	addl	$ARGS,%esp
	ret

	EPILOGUE
