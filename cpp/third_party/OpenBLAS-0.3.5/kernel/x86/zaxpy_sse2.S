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

#define STACK	16
#define ARGS	 0

#define STACK_M		 4 + STACK + ARGS(%esp)
#define STACK_ALPHA_R	16 + STACK + ARGS(%esp)
#define STACK_ALPHA_I	24 + STACK + ARGS(%esp)
#define STACK_X		32 + STACK + ARGS(%esp)
#define STACK_INCX	36 + STACK + ARGS(%esp)
#define STACK_Y		40 + STACK + ARGS(%esp)
#define STACK_INCY	44 + STACK + ARGS(%esp)

#define M	%ebx
#define X	%esi
#define	INCX	%ecx
#define Y	%edi
#define INCY	%edx
#define YY	%ebp

#define ALPHA_R	%xmm6
#define ALPHA_I	%xmm7

#if defined(HAVE_SSE3) && !defined(CORE_OPTERON)
#define MOVDDUP(a, b, c)	movddup	a(b), c
#define MOVDDUP2(a, b, c)	movddup	a##b, c
#else
#define MOVDDUP(a, b, c)	movlpd	a(b), c;movhpd	a(b), c
#define MOVDDUP2(a, b, c)	movlpd	a##b, c;movhpd	a##b, c
#endif

#include "l1param.h"

	PROLOGUE

	pushl	%ebp
	pushl	%edi
	pushl	%esi
	pushl	%ebx

	PROFCODE

	movl	STACK_M,    M
	movsd	STACK_ALPHA_R, %xmm0
	movsd	STACK_ALPHA_I, %xmm1
	movl	STACK_X,    X
	movl	STACK_INCX, INCX
	movl	STACK_Y,    Y
	movl	STACK_INCY, INCY

	sall	$ZBASE_SHIFT, INCX
	sall	$ZBASE_SHIFT, INCY

	testl	M, M
	jle	.L999

	cmpl	$2 * SIZE, INCX
	jne	.L50
	cmpl	$2 * SIZE, INCY
	jne	.L50

	subl	$-16 * SIZE, X
	subl	$-16 * SIZE, Y

	pcmpeqb	%xmm5, %xmm5
	psllq	$63,   %xmm5

#ifdef HAVE_SSE3
	movddup	%xmm0, ALPHA_R
	movddup	%xmm1, ALPHA_I
#else
	pshufd	$0x44, %xmm0, ALPHA_R
	pshufd	$0x44, %xmm1, ALPHA_I
#endif

#ifndef CONJ
	shufps	$0x0c, %xmm5, %xmm5
	xorpd	%xmm5, ALPHA_I
#else
	shufps	$0xc0, %xmm5, %xmm5
	xorpd	%xmm5, ALPHA_R
#endif

	testl	$SIZE, Y
	jne	.L30

	testl	$SIZE, X
	jne	.L20

	movl	M,  %eax
	sarl	$3, %eax
	jle	.L15

	movaps	-16 * SIZE(X), %xmm0
	movaps	-14 * SIZE(X), %xmm1
	movaps	-12 * SIZE(X), %xmm2
	movaps	-10 * SIZE(X), %xmm3

	decl	%eax
	jle	.L12
	ALIGN_3

.L11:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	pshufd	$0x4e,   %xmm0, %xmm5
	mulpd	ALPHA_R, %xmm0
	mulpd	ALPHA_I, %xmm5
	addpd	-16 * SIZE(Y), %xmm0
	addpd	%xmm5,   %xmm0
	movaps	%xmm0, -16 * SIZE(Y)
	movaps	 -8 * SIZE(X), %xmm0

	pshufd	$0x4e,   %xmm1, %xmm5
	mulpd	ALPHA_R, %xmm1
	mulpd	ALPHA_I, %xmm5
	addpd	-14 * SIZE(Y), %xmm1
	addpd	%xmm5,   %xmm1
	movaps	%xmm1, -14 * SIZE(Y)
	movaps	 -6 * SIZE(X), %xmm1

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	pshufd	$0x4e,   %xmm2, %xmm5
	mulpd	ALPHA_R, %xmm2
	mulpd	ALPHA_I, %xmm5
	addpd	-12 * SIZE(Y), %xmm2
	addpd	%xmm5,   %xmm2
	movaps	%xmm2, -12 * SIZE(Y)
	movaps	 -4 * SIZE(X), %xmm2

	pshufd	$0x4e,   %xmm3, %xmm5
	mulpd	ALPHA_R, %xmm3
	mulpd	ALPHA_I, %xmm5
	addpd	-10 * SIZE(Y), %xmm3
	addpd	%xmm5,   %xmm3
	movaps	%xmm3, -10 * SIZE(Y)
	movaps	 -2 * SIZE(X), %xmm3

#if defined(PREFETCHW) && !defined(FETCH128)
	PREFETCHW (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	pshufd	$0x4e,   %xmm0, %xmm5
	mulpd	ALPHA_R, %xmm0
	mulpd	ALPHA_I, %xmm5
	addpd	 -8 * SIZE(Y), %xmm0
	addpd	%xmm5,   %xmm0
	movaps	%xmm0,  -8 * SIZE(Y)
	movaps	  0 * SIZE(X), %xmm0

	pshufd	$0x4e,   %xmm1, %xmm5
	mulpd	ALPHA_R, %xmm1
	mulpd	ALPHA_I, %xmm5
	addpd	 -6 * SIZE(Y), %xmm1
	addpd	%xmm5,   %xmm1
	movaps	%xmm1,  -6 * SIZE(Y)
	movaps	  2 * SIZE(X), %xmm1

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	pshufd	$0x4e,   %xmm2, %xmm5
	mulpd	ALPHA_R, %xmm2
	mulpd	ALPHA_I, %xmm5
	addpd	 -4 * SIZE(Y), %xmm2
	addpd	%xmm5,   %xmm2
	movaps	%xmm2,  -4 * SIZE(Y)
	movaps	  4 * SIZE(X), %xmm2

	pshufd	$0x4e,   %xmm3, %xmm5
	mulpd	ALPHA_R, %xmm3
	mulpd	ALPHA_I, %xmm5
	addpd	 -2 * SIZE(Y), %xmm3
	addpd	%xmm5,   %xmm3
	movaps	%xmm3,  -2 * SIZE(Y)
	movaps	  6 * SIZE(X), %xmm3

	subl	$-16 * SIZE, X
	subl	$-16 * SIZE, Y
	decl	%eax
	jg	.L11
	ALIGN_3

.L12:
	pshufd	$0x4e,   %xmm0, %xmm5
	mulpd	ALPHA_R, %xmm0
	mulpd	ALPHA_I, %xmm5
	addpd	-16 * SIZE(Y), %xmm0
	addpd	%xmm5,   %xmm0
	movaps	%xmm0, -16 * SIZE(Y)
	movaps	 -8 * SIZE(X), %xmm0

	pshufd	$0x4e,   %xmm1, %xmm5
	mulpd	ALPHA_R, %xmm1
	mulpd	ALPHA_I, %xmm5
	addpd	-14 * SIZE(Y), %xmm1
	addpd	%xmm5,   %xmm1
	movaps	%xmm1, -14 * SIZE(Y)
	movaps	 -6 * SIZE(X), %xmm1

	pshufd	$0x4e,   %xmm2, %xmm5
	mulpd	ALPHA_R, %xmm2
	mulpd	ALPHA_I, %xmm5
	addpd	-12 * SIZE(Y), %xmm2
	addpd	%xmm5,   %xmm2
	movaps	%xmm2, -12 * SIZE(Y)
	movaps	 -4 * SIZE(X), %xmm2

	pshufd	$0x4e,   %xmm3, %xmm5
	mulpd	ALPHA_R, %xmm3
	mulpd	ALPHA_I, %xmm5
	addpd	-10 * SIZE(Y), %xmm3
	addpd	%xmm5,   %xmm3
	movaps	%xmm3, -10 * SIZE(Y)
	movaps	 -2 * SIZE(X), %xmm3

	pshufd	$0x4e,   %xmm0, %xmm5
	mulpd	ALPHA_R, %xmm0
	mulpd	ALPHA_I, %xmm5
	addpd	 -8 * SIZE(Y), %xmm0
	addpd	%xmm5,   %xmm0
	movaps	%xmm0,  -8 * SIZE(Y)

	pshufd	$0x4e,   %xmm1, %xmm5
	mulpd	ALPHA_R, %xmm1
	mulpd	ALPHA_I, %xmm5
	addpd	 -6 * SIZE(Y), %xmm1
	addpd	%xmm5,   %xmm1
	movaps	%xmm1,  -6 * SIZE(Y)

	pshufd	$0x4e,   %xmm2, %xmm5
	mulpd	ALPHA_R, %xmm2
	mulpd	ALPHA_I, %xmm5
	addpd	 -4 * SIZE(Y), %xmm2
	addpd	%xmm5,   %xmm2
	movaps	%xmm2,  -4 * SIZE(Y)

	pshufd	$0x4e,   %xmm3, %xmm5
	mulpd	ALPHA_R, %xmm3
	mulpd	ALPHA_I, %xmm5
	addpd	 -2 * SIZE(Y), %xmm3
	addpd	%xmm5,   %xmm3
	movaps	%xmm3,  -2 * SIZE(Y)

	subl	$-16 * SIZE, X
	subl	$-16 * SIZE, Y
	ALIGN_3

.L15:
	movl	M,  %eax
	andl	$4, %eax
	jle	.L16

	movaps	-16 * SIZE(X), %xmm0
	movaps	-14 * SIZE(X), %xmm1
	movaps	-12 * SIZE(X), %xmm2
	movaps	-10 * SIZE(X), %xmm3

	pshufd	$0x4e,   %xmm0, %xmm5
	mulpd	ALPHA_R, %xmm0
	mulpd	ALPHA_I, %xmm5
	addpd	-16 * SIZE(Y), %xmm0
	addpd	%xmm5,   %xmm0
	movaps	%xmm0, -16 * SIZE(Y)

	pshufd	$0x4e,   %xmm1, %xmm5
	mulpd	ALPHA_R, %xmm1
	mulpd	ALPHA_I, %xmm5
	addpd	-14 * SIZE(Y), %xmm1
	addpd	%xmm5,   %xmm1
	movaps	%xmm1, -14 * SIZE(Y)

	pshufd	$0x4e,   %xmm2, %xmm5
	mulpd	ALPHA_R, %xmm2
	mulpd	ALPHA_I, %xmm5
	addpd	-12 * SIZE(Y), %xmm2
	addpd	%xmm5,   %xmm2
	movaps	%xmm2, -12 * SIZE(Y)

	pshufd	$0x4e,   %xmm3, %xmm5
	mulpd	ALPHA_R, %xmm3
	mulpd	ALPHA_I, %xmm5
	addpd	-10 * SIZE(Y), %xmm3
	addpd	%xmm5,   %xmm3
	movaps	%xmm3, -10 * SIZE(Y)

	addl	$8 * SIZE, X
	addl	$8 * SIZE, Y
	ALIGN_3

.L16:
	movl	M,  %eax
	andl	$2, %eax
	jle	.L17

	movaps	-16 * SIZE(X), %xmm0
	movaps	-14 * SIZE(X), %xmm1

	pshufd	$0x4e,   %xmm0, %xmm5
	mulpd	ALPHA_R, %xmm0
	mulpd	ALPHA_I, %xmm5
	addpd	-16 * SIZE(Y), %xmm0
	addpd	%xmm5,   %xmm0
	movaps	%xmm0, -16 * SIZE(Y)

	pshufd	$0x4e,   %xmm1, %xmm5
	mulpd	ALPHA_R, %xmm1
	mulpd	ALPHA_I, %xmm5
	addpd	-14 * SIZE(Y), %xmm1
	addpd	%xmm5,   %xmm1
	movaps	%xmm1, -14 * SIZE(Y)

	addl	$4 * SIZE, X
	addl	$4 * SIZE, Y
	ALIGN_3

.L17:
	movl	M,  %eax
	andl	$1, %eax
	jle	.L999

	movaps	-16 * SIZE(X), %xmm0

	pshufd	$0x4e,   %xmm0, %xmm5
	mulpd	ALPHA_R, %xmm0
	mulpd	ALPHA_I, %xmm5
	addpd	-16 * SIZE(Y), %xmm0
	addpd	%xmm5,   %xmm0
	movaps	%xmm0, -16 * SIZE(Y)
	jmp	.L999
	ALIGN_3

.L20:
	movl	M,  %eax
	sarl	$3, %eax
	jle	.L25

	movsd	-16 * SIZE(X), %xmm0
	movhps	-15 * SIZE(X), %xmm0
	movsd	-14 * SIZE(X), %xmm1
	movhps	-13 * SIZE(X), %xmm1
	movsd	-12 * SIZE(X), %xmm2
	movhps	-11 * SIZE(X), %xmm2
	movsd	-10 * SIZE(X), %xmm3
	movhps	 -9 * SIZE(X), %xmm3

	decl	%eax
	jle	.L22
	ALIGN_3

.L21:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	pshufd	$0x4e,   %xmm0, %xmm5
	mulpd	ALPHA_R, %xmm0
	mulpd	ALPHA_I, %xmm5
	addpd	-16 * SIZE(Y), %xmm0
	addpd	%xmm5,   %xmm0
	movaps	%xmm0, -16 * SIZE(Y)
	movsd	 -8 * SIZE(X), %xmm0
	movhps	 -7 * SIZE(X), %xmm0

	pshufd	$0x4e,   %xmm1, %xmm5
	mulpd	ALPHA_R, %xmm1
	mulpd	ALPHA_I, %xmm5
	addpd	-14 * SIZE(Y), %xmm1
	addpd	%xmm5,   %xmm1
	movaps	%xmm1, -14 * SIZE(Y)
	movsd	 -6 * SIZE(X), %xmm1
	movhps	 -5 * SIZE(X), %xmm1

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	pshufd	$0x4e,   %xmm2, %xmm5
	mulpd	ALPHA_R, %xmm2
	mulpd	ALPHA_I, %xmm5
	addpd	-12 * SIZE(Y), %xmm2
	addpd	%xmm5,   %xmm2
	movaps	%xmm2, -12 * SIZE(Y)
	movsd	 -4 * SIZE(X), %xmm2
	movhps	 -3 * SIZE(X), %xmm2

	pshufd	$0x4e,   %xmm3, %xmm5
	mulpd	ALPHA_R, %xmm3
	mulpd	ALPHA_I, %xmm5
	addpd	-10 * SIZE(Y), %xmm3
	addpd	%xmm5,   %xmm3
	movaps	%xmm3, -10 * SIZE(Y)
	movsd	 -2 * SIZE(X), %xmm3
	movhps	 -1 * SIZE(X), %xmm3

#if defined(PREFETCHW) && !defined(FETCH128)
	PREFETCHW (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	pshufd	$0x4e,   %xmm0, %xmm5
	mulpd	ALPHA_R, %xmm0
	mulpd	ALPHA_I, %xmm5
	addpd	 -8 * SIZE(Y), %xmm0
	addpd	%xmm5,   %xmm0
	movaps	%xmm0,  -8 * SIZE(Y)
	movsd	  0 * SIZE(X), %xmm0
	movhps	  1 * SIZE(X), %xmm0

	pshufd	$0x4e,   %xmm1, %xmm5
	mulpd	ALPHA_R, %xmm1
	mulpd	ALPHA_I, %xmm5
	addpd	 -6 * SIZE(Y), %xmm1
	addpd	%xmm5,   %xmm1
	movaps	%xmm1,  -6 * SIZE(Y)
	movsd	  2 * SIZE(X), %xmm1
	movhps	  3 * SIZE(X), %xmm1

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	pshufd	$0x4e,   %xmm2, %xmm5
	mulpd	ALPHA_R, %xmm2
	mulpd	ALPHA_I, %xmm5
	addpd	 -4 * SIZE(Y), %xmm2
	addpd	%xmm5,   %xmm2
	movaps	%xmm2,  -4 * SIZE(Y)
	movsd	  4 * SIZE(X), %xmm2
	movhps	  5 * SIZE(X), %xmm2

	pshufd	$0x4e,   %xmm3, %xmm5
	mulpd	ALPHA_R, %xmm3
	mulpd	ALPHA_I, %xmm5
	addpd	 -2 * SIZE(Y), %xmm3
	addpd	%xmm5,   %xmm3
	movaps	%xmm3,  -2 * SIZE(Y)
	movsd	  6 * SIZE(X), %xmm3
	movhps	  7 * SIZE(X), %xmm3

	subl	$-16 * SIZE, X
	subl	$-16 * SIZE, Y
	decl	%eax
	jg	.L21
	ALIGN_3

.L22:
	pshufd	$0x4e,   %xmm0, %xmm5
	mulpd	ALPHA_R, %xmm0
	mulpd	ALPHA_I, %xmm5
	addpd	-16 * SIZE(Y), %xmm0
	addpd	%xmm5,   %xmm0
	movaps	%xmm0, -16 * SIZE(Y)
	movsd	 -8 * SIZE(X), %xmm0
	movhps	 -7 * SIZE(X), %xmm0

	pshufd	$0x4e,   %xmm1, %xmm5
	mulpd	ALPHA_R, %xmm1
	mulpd	ALPHA_I, %xmm5
	addpd	-14 * SIZE(Y), %xmm1
	addpd	%xmm5,   %xmm1
	movaps	%xmm1, -14 * SIZE(Y)
	movsd	 -6 * SIZE(X), %xmm1
	movhps	 -5 * SIZE(X), %xmm1

	pshufd	$0x4e,   %xmm2, %xmm5
	mulpd	ALPHA_R, %xmm2
	mulpd	ALPHA_I, %xmm5
	addpd	-12 * SIZE(Y), %xmm2
	addpd	%xmm5,   %xmm2
	movaps	%xmm2, -12 * SIZE(Y)
	movsd	 -4 * SIZE(X), %xmm2
	movhps	 -3 * SIZE(X), %xmm2

	pshufd	$0x4e,   %xmm3, %xmm5
	mulpd	ALPHA_R, %xmm3
	mulpd	ALPHA_I, %xmm5
	addpd	-10 * SIZE(Y), %xmm3
	addpd	%xmm5,   %xmm3
	movaps	%xmm3, -10 * SIZE(Y)
	movsd	 -2 * SIZE(X), %xmm3
	movhps	 -1 * SIZE(X), %xmm3

	pshufd	$0x4e,   %xmm0, %xmm5
	mulpd	ALPHA_R, %xmm0
	mulpd	ALPHA_I, %xmm5
	addpd	 -8 * SIZE(Y), %xmm0
	addpd	%xmm5,   %xmm0
	movaps	%xmm0,  -8 * SIZE(Y)

	pshufd	$0x4e,   %xmm1, %xmm5
	mulpd	ALPHA_R, %xmm1
	mulpd	ALPHA_I, %xmm5
	addpd	 -6 * SIZE(Y), %xmm1
	addpd	%xmm5,   %xmm1
	movaps	%xmm1,  -6 * SIZE(Y)

	pshufd	$0x4e,   %xmm2, %xmm5
	mulpd	ALPHA_R, %xmm2
	mulpd	ALPHA_I, %xmm5
	addpd	 -4 * SIZE(Y), %xmm2
	addpd	%xmm5,   %xmm2
	movaps	%xmm2,  -4 * SIZE(Y)

	pshufd	$0x4e,   %xmm3, %xmm5
	mulpd	ALPHA_R, %xmm3
	mulpd	ALPHA_I, %xmm5
	addpd	 -2 * SIZE(Y), %xmm3
	addpd	%xmm5,   %xmm3
	movaps	%xmm3,  -2 * SIZE(Y)

	subl	$-16 * SIZE, X
	subl	$-16 * SIZE, Y
	ALIGN_3

.L25:
	movl	M,  %eax
	andl	$4, %eax
	jle	.L26

	movsd	-16 * SIZE(X), %xmm0
	movhps	-15 * SIZE(X), %xmm0
	movsd	-14 * SIZE(X), %xmm1
	movhps	-13 * SIZE(X), %xmm1

	pshufd	$0x4e,   %xmm0, %xmm5
	mulpd	ALPHA_R, %xmm0
	mulpd	ALPHA_I, %xmm5
	addpd	-16 * SIZE(Y), %xmm0
	addpd	%xmm5,   %xmm0
	movaps	%xmm0, -16 * SIZE(Y)

	pshufd	$0x4e,   %xmm1, %xmm5
	mulpd	ALPHA_R, %xmm1
	mulpd	ALPHA_I, %xmm5
	addpd	-14 * SIZE(Y), %xmm1
	addpd	%xmm5,   %xmm1
	movaps	%xmm1, -14 * SIZE(Y)

	movsd	-12 * SIZE(X), %xmm2
	movhps	-11 * SIZE(X), %xmm2
	movsd	-10 * SIZE(X), %xmm3
	movhps	 -9 * SIZE(X), %xmm3

	pshufd	$0x4e,   %xmm2, %xmm5
	mulpd	ALPHA_R, %xmm2
	mulpd	ALPHA_I, %xmm5
	addpd	-12 * SIZE(Y), %xmm2
	addpd	%xmm5,   %xmm2
	movaps	%xmm2, -12 * SIZE(Y)

	pshufd	$0x4e,   %xmm3, %xmm5
	mulpd	ALPHA_R, %xmm3
	mulpd	ALPHA_I, %xmm5
	addpd	-10 * SIZE(Y), %xmm3
	addpd	%xmm5,   %xmm3
	movaps	%xmm3, -10 * SIZE(Y)

	addl	$8 * SIZE, X
	addl	$8 * SIZE, Y
	ALIGN_3

.L26:
	movl	M,  %eax
	andl	$2, %eax
	jle	.L27

	movsd	-16 * SIZE(X), %xmm0
	movhps	-15 * SIZE(X), %xmm0

	pshufd	$0x4e,   %xmm0, %xmm5
	mulpd	ALPHA_R, %xmm0
	mulpd	ALPHA_I, %xmm5
	addpd	-16 * SIZE(Y), %xmm0
	addpd	%xmm5,   %xmm0
	movaps	%xmm0, -16 * SIZE(Y)

	movsd	-14 * SIZE(X), %xmm1
	movhps	-13 * SIZE(X), %xmm1

	pshufd	$0x4e,   %xmm1, %xmm5
	mulpd	ALPHA_R, %xmm1
	mulpd	ALPHA_I, %xmm5
	addpd	-14 * SIZE(Y), %xmm1
	addpd	%xmm5,   %xmm1
	movaps	%xmm1, -14 * SIZE(Y)

	addl	$4 * SIZE, X
	addl	$4 * SIZE, Y
	ALIGN_3

.L27:
	movl	M,  %eax
	andl	$1, %eax
	jle	.L999

	movsd	-16 * SIZE(X), %xmm0
	movhps	-15 * SIZE(X), %xmm0

	pshufd	$0x4e,   %xmm0, %xmm5
	mulpd	ALPHA_R, %xmm0
	mulpd	ALPHA_I, %xmm5
	addpd	-16 * SIZE(Y), %xmm0
	addpd	%xmm5,   %xmm0
	movaps	%xmm0, -16 * SIZE(Y)
	jmp	.L999
	ALIGN_3

.L30:
	testl	$SIZE, X
	jne	.L40

	movaps	-16 * SIZE(X), %xmm1

	pshufd	$0x4e,   %xmm1, %xmm5
	mulpd	ALPHA_R, %xmm1
	mulpd	ALPHA_I, %xmm5
	addpd	%xmm5,   %xmm1
	xorps	%xmm0,   %xmm0
	SHUFPD_1 %xmm1,  %xmm0

	xorps	%xmm4,   %xmm4
	movhps	-16 * SIZE(Y), %xmm4

	addpd	%xmm0,   %xmm4
	movhps	%xmm4, -16 * SIZE(Y)
	movaps	%xmm1,   %xmm0

	addl	$2 * SIZE, X
	addl	$1 * SIZE, Y
	decl	M
	jle	.L39

	movl	M,  %eax
	sarl	$3, %eax
	jle	.L35

	movaps	-16 * SIZE(X), %xmm1
	movaps	-14 * SIZE(X), %xmm2
	movaps	-12 * SIZE(X), %xmm3

	decl	%eax
	jle	.L32
	ALIGN_3

.L31:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	pshufd	$0x4e,   %xmm1, %xmm5
	mulpd	ALPHA_R, %xmm1
	mulpd	ALPHA_I, %xmm5
	addpd	%xmm5,   %xmm1
	SHUFPD_1 %xmm1,  %xmm0
	addpd	-16 * SIZE(Y), %xmm0
	movaps	%xmm0, -16 * SIZE(Y)
	movaps	-10 * SIZE(X), %xmm0

	pshufd	$0x4e,   %xmm2, %xmm5
	mulpd	ALPHA_R, %xmm2
	mulpd	ALPHA_I, %xmm5
	addpd	%xmm5,   %xmm2
	SHUFPD_1 %xmm2,  %xmm1
	addpd	-14 * SIZE(Y), %xmm1
	movaps	%xmm1, -14 * SIZE(Y)
	movaps	 -8 * SIZE(X), %xmm1

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	pshufd	$0x4e,   %xmm3, %xmm5
	mulpd	ALPHA_R, %xmm3
	mulpd	ALPHA_I, %xmm5
	addpd	%xmm5,   %xmm3
	SHUFPD_1 %xmm3,  %xmm2
	addpd	-12 * SIZE(Y), %xmm2
	movaps	%xmm2, -12 * SIZE(Y)
	movaps	 -6 * SIZE(X), %xmm2

	pshufd	$0x4e,   %xmm0, %xmm5
	mulpd	ALPHA_R, %xmm0
	mulpd	ALPHA_I, %xmm5
	addpd	%xmm5,   %xmm0
	SHUFPD_1 %xmm0,  %xmm3
	addpd	-10 * SIZE(Y), %xmm3
	movaps	%xmm3, -10 * SIZE(Y)
	movaps	 -4 * SIZE(X), %xmm3

#if defined(PREFETCHW) && !defined(FETCH128)
	PREFETCHW (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	pshufd	$0x4e,   %xmm1, %xmm5
	mulpd	ALPHA_R, %xmm1
	mulpd	ALPHA_I, %xmm5
	addpd	%xmm5,   %xmm1
	SHUFPD_1 %xmm1,  %xmm0
	addpd	 -8 * SIZE(Y), %xmm0
	movaps	%xmm0,  -8 * SIZE(Y)
	movaps	 -2 * SIZE(X), %xmm0

	pshufd	$0x4e,   %xmm2, %xmm5
	mulpd	ALPHA_R, %xmm2
	mulpd	ALPHA_I, %xmm5
	addpd	%xmm5,   %xmm2
	SHUFPD_1 %xmm2,  %xmm1
	addpd	 -6 * SIZE(Y), %xmm1
	movaps	%xmm1,  -6 * SIZE(Y)
	movaps	  0 * SIZE(X), %xmm1

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	pshufd	$0x4e,   %xmm3, %xmm5
	mulpd	ALPHA_R, %xmm3
	mulpd	ALPHA_I, %xmm5
	addpd	%xmm5,   %xmm3
	SHUFPD_1 %xmm3,  %xmm2
	addpd	 -4 * SIZE(Y), %xmm2
	movaps	%xmm2,  -4 * SIZE(Y)
	movaps	  2 * SIZE(X), %xmm2

	pshufd	$0x4e,   %xmm0, %xmm5
	mulpd	ALPHA_R, %xmm0
	mulpd	ALPHA_I, %xmm5
	addpd	%xmm5,   %xmm0
	SHUFPD_1 %xmm0,  %xmm3
	addpd	 -2 * SIZE(Y), %xmm3
	movaps	%xmm3,  -2 * SIZE(Y)
	movaps	  4 * SIZE(X), %xmm3

	subl	$-16 * SIZE, X
	subl	$-16 * SIZE, Y
	decl	%eax
	jg	.L31
	ALIGN_3

.L32:
	pshufd	$0x4e,   %xmm1, %xmm5
	mulpd	ALPHA_R, %xmm1
	mulpd	ALPHA_I, %xmm5
	addpd	%xmm5,   %xmm1
	SHUFPD_1 %xmm1,  %xmm0
	addpd	-16 * SIZE(Y), %xmm0
	movaps	%xmm0, -16 * SIZE(Y)
	movaps	-10 * SIZE(X), %xmm0

	pshufd	$0x4e,   %xmm2, %xmm5
	mulpd	ALPHA_R, %xmm2
	mulpd	ALPHA_I, %xmm5
	addpd	%xmm5,   %xmm2
	SHUFPD_1 %xmm2,  %xmm1
	addpd	-14 * SIZE(Y), %xmm1
	movaps	%xmm1, -14 * SIZE(Y)
	movaps	 -8 * SIZE(X), %xmm1

	pshufd	$0x4e,   %xmm3, %xmm5
	mulpd	ALPHA_R, %xmm3
	mulpd	ALPHA_I, %xmm5
	addpd	%xmm5,   %xmm3
	SHUFPD_1 %xmm3,  %xmm2
	addpd	-12 * SIZE(Y), %xmm2
	movaps	%xmm2, -12 * SIZE(Y)
	movaps	 -6 * SIZE(X), %xmm2

	pshufd	$0x4e,   %xmm0, %xmm5
	mulpd	ALPHA_R, %xmm0
	mulpd	ALPHA_I, %xmm5
	addpd	%xmm5,   %xmm0
	SHUFPD_1 %xmm0,  %xmm3
	addpd	-10 * SIZE(Y), %xmm3
	movaps	%xmm3, -10 * SIZE(Y)
	movaps	 -4 * SIZE(X), %xmm3

	pshufd	$0x4e,   %xmm1, %xmm5
	mulpd	ALPHA_R, %xmm1
	mulpd	ALPHA_I, %xmm5
	addpd	%xmm5,   %xmm1
	SHUFPD_1 %xmm1,  %xmm0
	addpd	 -8 * SIZE(Y), %xmm0
	movaps	%xmm0,  -8 * SIZE(Y)
	movaps	 -2 * SIZE(X), %xmm0

	pshufd	$0x4e,   %xmm2, %xmm5
	mulpd	ALPHA_R, %xmm2
	mulpd	ALPHA_I, %xmm5
	addpd	%xmm5,   %xmm2
	SHUFPD_1 %xmm2,  %xmm1
	addpd	 -6 * SIZE(Y), %xmm1
	movaps	%xmm1,  -6 * SIZE(Y)

	pshufd	$0x4e,   %xmm3, %xmm5
	mulpd	ALPHA_R, %xmm3
	mulpd	ALPHA_I, %xmm5
	addpd	%xmm5,   %xmm3
	SHUFPD_1 %xmm3,  %xmm2
	addpd	 -4 * SIZE(Y), %xmm2
	movaps	%xmm2,  -4 * SIZE(Y)

	pshufd	$0x4e,   %xmm0, %xmm5
	mulpd	ALPHA_R, %xmm0
	mulpd	ALPHA_I, %xmm5
	addpd	%xmm5,   %xmm0
	SHUFPD_1 %xmm0,  %xmm3
	addpd	 -2 * SIZE(Y), %xmm3
	movaps	%xmm3,  -2 * SIZE(Y)

	subl	$-16 * SIZE, X
	subl	$-16 * SIZE, Y
	ALIGN_3

.L35:
	movl	M,  %eax
	andl	$4, %eax
	jle	.L36

	movaps	-16 * SIZE(X), %xmm1
	movaps	-14 * SIZE(X), %xmm2
	movaps	-12 * SIZE(X), %xmm3
	movaps	-10 * SIZE(X), %xmm4

	pshufd	$0x4e,   %xmm1, %xmm5
	mulpd	ALPHA_R, %xmm1
	mulpd	ALPHA_I, %xmm5
	addpd	%xmm5,   %xmm1
	SHUFPD_1 %xmm1,  %xmm0
	addpd	-16 * SIZE(Y), %xmm0
	movaps	%xmm0, -16 * SIZE(Y)

	pshufd	$0x4e,   %xmm2, %xmm5
	mulpd	ALPHA_R, %xmm2
	mulpd	ALPHA_I, %xmm5
	addpd	%xmm5,   %xmm2
	SHUFPD_1 %xmm2,  %xmm1
	addpd	-14 * SIZE(Y), %xmm1
	movaps	%xmm1, -14 * SIZE(Y)

	pshufd	$0x4e,   %xmm3, %xmm5
	mulpd	ALPHA_R, %xmm3
	mulpd	ALPHA_I, %xmm5
	addpd	%xmm5,   %xmm3
	SHUFPD_1 %xmm3,  %xmm2
	addpd	-12 * SIZE(Y), %xmm2
	movaps	%xmm2, -12 * SIZE(Y)

	pshufd	$0x4e,   %xmm4, %xmm5
	mulpd	ALPHA_R, %xmm4
	mulpd	ALPHA_I, %xmm5
	addpd	%xmm5,   %xmm4
	SHUFPD_1 %xmm4,  %xmm3
	addpd	-10 * SIZE(Y), %xmm3
	movaps	%xmm3, -10 * SIZE(Y)

	movaps	%xmm4, %xmm0

	addl	$8 * SIZE, X
	addl	$8 * SIZE, Y
	ALIGN_3

.L36:
	movl	M,  %eax
	andl	$2, %eax
	jle	.L37

	movaps	-16 * SIZE(X), %xmm1
	movaps	-14 * SIZE(X), %xmm2

	pshufd	$0x4e,   %xmm1, %xmm5
	mulpd	ALPHA_R, %xmm1
	mulpd	ALPHA_I, %xmm5
	addpd	%xmm5,   %xmm1
	SHUFPD_1 %xmm1,  %xmm0
	addpd	-16 * SIZE(Y), %xmm0
	movaps	%xmm0, -16 * SIZE(Y)

	pshufd	$0x4e,   %xmm2, %xmm5
	mulpd	ALPHA_R, %xmm2
	mulpd	ALPHA_I, %xmm5
	addpd	%xmm5,   %xmm2
	SHUFPD_1 %xmm2,  %xmm1
	addpd	-14 * SIZE(Y), %xmm1
	movaps	%xmm1, -14 * SIZE(Y)

	movaps	%xmm2, %xmm0

	addl	$4 * SIZE, X
	addl	$4 * SIZE, Y
	ALIGN_3

.L37:
	movl	M,  %eax
	andl	$1, %eax
	jle	.L39

	movaps	-16 * SIZE(X), %xmm1

	pshufd	$0x4e,   %xmm1, %xmm5
	mulpd	ALPHA_R, %xmm1
	mulpd	ALPHA_I, %xmm5
	addpd	%xmm5,   %xmm1
	SHUFPD_1 %xmm1,  %xmm0

	addpd	-16 * SIZE(Y), %xmm0
	movaps	%xmm0, -16 * SIZE(Y)
	movaps	%xmm1,   %xmm0

	addl	$2 * SIZE, X
	addl	$2 * SIZE, Y
	ALIGN_3

.L39:
	SHUFPD_1 %xmm0,  %xmm0

	addsd	-16 * SIZE(Y), %xmm0
	movlps	%xmm0, -16 * SIZE(Y)
	jmp	.L999
	ALIGN_3

.L40:
	movsd	-16 * SIZE(X), %xmm1
	movhps	-15 * SIZE(X), %xmm1

	pshufd	$0x4e,   %xmm1, %xmm5
	mulpd	ALPHA_R, %xmm1
	mulpd	ALPHA_I, %xmm5
	addpd	%xmm5,   %xmm1
	xorps	%xmm0,   %xmm0
	SHUFPD_1 %xmm1,  %xmm0

	xorps	%xmm4,   %xmm4
	movhps	-16 * SIZE(Y), %xmm4

	addpd	%xmm0,   %xmm4
	movhps	%xmm4, -16 * SIZE(Y)
	movaps	%xmm1,   %xmm0

	addl	$2 * SIZE, X
	addl	$1 * SIZE, Y
	decl	M
	jle	.L49

	movl	M,  %eax
	sarl	$3, %eax
	jle	.L45

	movsd	-16 * SIZE(X), %xmm1
	movhps	-15 * SIZE(X), %xmm1
	movsd	-14 * SIZE(X), %xmm2
	movhps	-13 * SIZE(X), %xmm2
	movsd	-12 * SIZE(X), %xmm3
	movhps	-11 * SIZE(X), %xmm3

	decl	%eax
	jle	.L42
	ALIGN_3

.L41:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	pshufd	$0x4e,   %xmm1, %xmm5
	mulpd	ALPHA_R, %xmm1
	mulpd	ALPHA_I, %xmm5
	addpd	%xmm5,   %xmm1
	SHUFPD_1 %xmm1,  %xmm0
	addpd	-16 * SIZE(Y), %xmm0
	movaps	%xmm0, -16 * SIZE(Y)
	movsd	-10 * SIZE(X), %xmm0
	movhps	 -9 * SIZE(X), %xmm0

	pshufd	$0x4e,   %xmm2, %xmm5
	mulpd	ALPHA_R, %xmm2
	mulpd	ALPHA_I, %xmm5
	addpd	%xmm5,   %xmm2
	SHUFPD_1 %xmm2,  %xmm1
	addpd	-14 * SIZE(Y), %xmm1
	movaps	%xmm1, -14 * SIZE(Y)
	movsd	 -8 * SIZE(X), %xmm1
	movhps	 -7 * SIZE(X), %xmm1

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	pshufd	$0x4e,   %xmm3, %xmm5
	mulpd	ALPHA_R, %xmm3
	mulpd	ALPHA_I, %xmm5
	addpd	%xmm5,   %xmm3
	SHUFPD_1 %xmm3,  %xmm2
	addpd	-12 * SIZE(Y), %xmm2
	movaps	%xmm2, -12 * SIZE(Y)
	movsd	 -6 * SIZE(X), %xmm2
	movhps	 -5 * SIZE(X), %xmm2

	pshufd	$0x4e,   %xmm0, %xmm5
	mulpd	ALPHA_R, %xmm0
	mulpd	ALPHA_I, %xmm5
	addpd	%xmm5,   %xmm0
	SHUFPD_1 %xmm0,  %xmm3
	addpd	-10 * SIZE(Y), %xmm3
	movaps	%xmm3, -10 * SIZE(Y)
	movsd	 -4 * SIZE(X), %xmm3
	movhps	 -3 * SIZE(X), %xmm3

#if defined(PREFETCHW) && !defined(FETCH128)
	PREFETCHW (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	pshufd	$0x4e,   %xmm1, %xmm5
	mulpd	ALPHA_R, %xmm1
	mulpd	ALPHA_I, %xmm5
	addpd	%xmm5,   %xmm1
	SHUFPD_1 %xmm1,  %xmm0
	addpd	 -8 * SIZE(Y), %xmm0
	movaps	%xmm0,  -8 * SIZE(Y)
	movsd	 -2 * SIZE(X), %xmm0
	movhps	 -1 * SIZE(X), %xmm0

	pshufd	$0x4e,   %xmm2, %xmm5
	mulpd	ALPHA_R, %xmm2
	mulpd	ALPHA_I, %xmm5
	addpd	%xmm5,   %xmm2
	SHUFPD_1 %xmm2,  %xmm1
	addpd	 -6 * SIZE(Y), %xmm1
	movaps	%xmm1,  -6 * SIZE(Y)
	movsd	  0 * SIZE(X), %xmm1
	movhps	  1 * SIZE(X), %xmm1

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	pshufd	$0x4e,   %xmm3, %xmm5
	mulpd	ALPHA_R, %xmm3
	mulpd	ALPHA_I, %xmm5
	addpd	%xmm5,   %xmm3
	SHUFPD_1 %xmm3,  %xmm2
	addpd	 -4 * SIZE(Y), %xmm2
	movaps	%xmm2,  -4 * SIZE(Y)
	movsd	  2 * SIZE(X), %xmm2
	movhps	  3 * SIZE(X), %xmm2

	pshufd	$0x4e,   %xmm0, %xmm5
	mulpd	ALPHA_R, %xmm0
	mulpd	ALPHA_I, %xmm5
	addpd	%xmm5,   %xmm0
	SHUFPD_1 %xmm0,  %xmm3
	addpd	 -2 * SIZE(Y), %xmm3
	movaps	%xmm3,  -2 * SIZE(Y)
	movsd	  4 * SIZE(X), %xmm3
	movhps	  5 * SIZE(X), %xmm3

	subl	$-16 * SIZE, X
	subl	$-16 * SIZE, Y
	decl	%eax
	jg	.L41
	ALIGN_3

.L42:
	pshufd	$0x4e,   %xmm1, %xmm5
	mulpd	ALPHA_R, %xmm1
	mulpd	ALPHA_I, %xmm5
	addpd	%xmm5,   %xmm1
	SHUFPD_1 %xmm1,  %xmm0
	addpd	-16 * SIZE(Y), %xmm0
	movaps	%xmm0, -16 * SIZE(Y)
	movsd	-10 * SIZE(X), %xmm0
	movhps	 -9 * SIZE(X), %xmm0

	pshufd	$0x4e,   %xmm2, %xmm5
	mulpd	ALPHA_R, %xmm2
	mulpd	ALPHA_I, %xmm5
	addpd	%xmm5,   %xmm2
	SHUFPD_1 %xmm2,  %xmm1
	addpd	-14 * SIZE(Y), %xmm1
	movaps	%xmm1, -14 * SIZE(Y)
	movsd	 -8 * SIZE(X), %xmm1
	movhps	 -7 * SIZE(X), %xmm1

	pshufd	$0x4e,   %xmm3, %xmm5
	mulpd	ALPHA_R, %xmm3
	mulpd	ALPHA_I, %xmm5
	addpd	%xmm5,   %xmm3
	SHUFPD_1 %xmm3,  %xmm2
	addpd	-12 * SIZE(Y), %xmm2
	movaps	%xmm2, -12 * SIZE(Y)
	movsd	 -6 * SIZE(X), %xmm2
	movhps	 -5 * SIZE(X), %xmm2

	pshufd	$0x4e,   %xmm0, %xmm5
	mulpd	ALPHA_R, %xmm0
	mulpd	ALPHA_I, %xmm5
	addpd	%xmm5,   %xmm0
	SHUFPD_1 %xmm0,  %xmm3
	addpd	-10 * SIZE(Y), %xmm3
	movaps	%xmm3, -10 * SIZE(Y)
	movsd	 -4 * SIZE(X), %xmm3
	movhps	 -3 * SIZE(X), %xmm3

	pshufd	$0x4e,   %xmm1, %xmm5
	mulpd	ALPHA_R, %xmm1
	mulpd	ALPHA_I, %xmm5
	addpd	%xmm5,   %xmm1
	SHUFPD_1 %xmm1,  %xmm0
	addpd	 -8 * SIZE(Y), %xmm0
	movaps	%xmm0,  -8 * SIZE(Y)
	movsd	 -2 * SIZE(X), %xmm0
	movhps	 -1 * SIZE(X), %xmm0

	pshufd	$0x4e,   %xmm2, %xmm5
	mulpd	ALPHA_R, %xmm2
	mulpd	ALPHA_I, %xmm5
	addpd	%xmm5,   %xmm2
	SHUFPD_1 %xmm2,  %xmm1
	addpd	 -6 * SIZE(Y), %xmm1
	movaps	%xmm1,  -6 * SIZE(Y)

	pshufd	$0x4e,   %xmm3, %xmm5
	mulpd	ALPHA_R, %xmm3
	mulpd	ALPHA_I, %xmm5
	addpd	%xmm5,   %xmm3
	SHUFPD_1 %xmm3,  %xmm2
	addpd	 -4 * SIZE(Y), %xmm2
	movaps	%xmm2,  -4 * SIZE(Y)

	pshufd	$0x4e,   %xmm0, %xmm5
	mulpd	ALPHA_R, %xmm0
	mulpd	ALPHA_I, %xmm5
	addpd	%xmm5,   %xmm0
	SHUFPD_1 %xmm0,  %xmm3
	addpd	 -2 * SIZE(Y), %xmm3
	movaps	%xmm3,  -2 * SIZE(Y)

	subl	$-16 * SIZE, X
	subl	$-16 * SIZE, Y
	ALIGN_3

.L45:
	movl	M,  %eax
	andl	$4, %eax
	jle	.L46

	movsd	-16 * SIZE(X), %xmm1
	movhps	-15 * SIZE(X), %xmm1
	movsd	-14 * SIZE(X), %xmm2
	movhps	-13 * SIZE(X), %xmm2

	pshufd	$0x4e,   %xmm1, %xmm5
	mulpd	ALPHA_R, %xmm1
	mulpd	ALPHA_I, %xmm5
	addpd	%xmm5,   %xmm1
	SHUFPD_1 %xmm1,  %xmm0
	addpd	-16 * SIZE(Y), %xmm0
	movaps	%xmm0, -16 * SIZE(Y)

	pshufd	$0x4e,   %xmm2, %xmm5
	mulpd	ALPHA_R, %xmm2
	mulpd	ALPHA_I, %xmm5
	addpd	%xmm5,   %xmm2
	SHUFPD_1 %xmm2,  %xmm1
	addpd	-14 * SIZE(Y), %xmm1
	movaps	%xmm1, -14 * SIZE(Y)

	movsd	-12 * SIZE(X), %xmm3
	movhps	-11 * SIZE(X), %xmm3
	movsd	-10 * SIZE(X), %xmm4
	movhps	 -9 * SIZE(X), %xmm4

	pshufd	$0x4e,   %xmm3, %xmm5
	mulpd	ALPHA_R, %xmm3
	mulpd	ALPHA_I, %xmm5
	addpd	%xmm5,   %xmm3
	SHUFPD_1 %xmm3,  %xmm2
	addpd	-12 * SIZE(Y), %xmm2
	movaps	%xmm2, -12 * SIZE(Y)

	pshufd	$0x4e,   %xmm4, %xmm5
	mulpd	ALPHA_R, %xmm4
	mulpd	ALPHA_I, %xmm5
	addpd	%xmm5,   %xmm4
	SHUFPD_1 %xmm4,  %xmm3
	addpd	-10 * SIZE(Y), %xmm3
	movaps	%xmm3, -10 * SIZE(Y)

	movaps	%xmm4, %xmm0

	addl	$8 * SIZE, X
	addl	$8 * SIZE, Y
	ALIGN_3

.L46:
	movl	M,  %eax
	andl	$2, %eax
	jle	.L47

	movsd	-16 * SIZE(X), %xmm1
	movhps	-15 * SIZE(X), %xmm1
	movsd	-14 * SIZE(X), %xmm2
	movhps	-13 * SIZE(X), %xmm2

	pshufd	$0x4e,   %xmm1, %xmm5
	mulpd	ALPHA_R, %xmm1
	mulpd	ALPHA_I, %xmm5
	addpd	%xmm5,   %xmm1
	SHUFPD_1 %xmm1,  %xmm0
	addpd	-16 * SIZE(Y), %xmm0
	movaps	%xmm0, -16 * SIZE(Y)

	pshufd	$0x4e,   %xmm2, %xmm5
	mulpd	ALPHA_R, %xmm2
	mulpd	ALPHA_I, %xmm5
	addpd	%xmm5,   %xmm2
	SHUFPD_1 %xmm2,  %xmm1
	addpd	-14 * SIZE(Y), %xmm1
	movaps	%xmm1, -14 * SIZE(Y)

	movaps	%xmm2, %xmm0

	addl	$4 * SIZE, X
	addl	$4 * SIZE, Y
	ALIGN_3

.L47:
	movl	M,  %eax
	andl	$1, %eax
	jle	.L49

	movsd	-16 * SIZE(X), %xmm1
	movhps	-15 * SIZE(X), %xmm1

	pshufd	$0x4e,   %xmm1, %xmm5
	mulpd	ALPHA_R, %xmm1
	mulpd	ALPHA_I, %xmm5
	addpd	%xmm5,   %xmm1
	SHUFPD_1 %xmm1,  %xmm0

	addpd	-16 * SIZE(Y), %xmm0
	movaps	%xmm0, -16 * SIZE(Y)
	movaps	%xmm1, %xmm0
	addl	$2 * SIZE, Y
	ALIGN_3

.L49:
	SHUFPD_1 %xmm0,  %xmm0

	addsd	-16 * SIZE(Y), %xmm0
	movlps	%xmm0, -16 * SIZE(Y)
	jmp	.L999
	ALIGN_3

.L50:
#ifndef CONJ
	movaps	%xmm0, ALPHA_R

	pxor	ALPHA_I, ALPHA_I
	subsd	%xmm1,   ALPHA_I

	unpcklpd ALPHA_R, ALPHA_I
	unpcklpd %xmm1,   ALPHA_R
#else
	movaps	%xmm0, ALPHA_R
	movaps	%xmm1, ALPHA_I

	pxor	%xmm5, %xmm5
	subsd	%xmm0, %xmm5

	unpcklpd %xmm5, ALPHA_I
	unpcklpd %xmm1, ALPHA_R
#endif

	movl	Y, YY
	movl	M,  %eax
//If incx==0 || incy==0, avoid unloop and jump to end.
	cmpl	$0, INCX
	je  .L58
	cmpl	$0, INCY
	je	.L58

	sarl	$2, %eax
	jle	.L55

	MOVDDUP( 0 * SIZE, X, %xmm0)
	MOVDDUP( 1 * SIZE, X, %xmm1)
	addl	INCX, X
	MOVDDUP( 0 * SIZE, X, %xmm2)
	MOVDDUP( 1 * SIZE, X, %xmm3)
	addl	INCX, X

	movsd	 0 * SIZE(Y), %xmm4
	movhpd	 1 * SIZE(Y), %xmm4
	addl	 INCY, Y
	movsd	 0 * SIZE(Y), %xmm5
	movhpd	 1 * SIZE(Y), %xmm5
	addl	 INCY, Y

	decl	%eax
	jle	.L52
	ALIGN_3

.L51:
	mulpd	ALPHA_R, %xmm0
	mulpd	ALPHA_R, %xmm2
	mulpd	ALPHA_I, %xmm1
	mulpd	ALPHA_I, %xmm3

	addpd	%xmm0, %xmm4
	addpd	%xmm2, %xmm5
	addpd	%xmm1, %xmm4
	addpd	%xmm3, %xmm5

	movlpd	%xmm4,   0 * SIZE(YY)
	movhpd	%xmm4,   1 * SIZE(YY)
	addl	 INCY, YY
	movlpd	%xmm5,   0 * SIZE(YY)
	movhpd	%xmm5,   1 * SIZE(YY)
	addl	 INCY, YY

	MOVDDUP( 0 * SIZE, X, %xmm0)
	MOVDDUP( 1 * SIZE, X, %xmm1)
	addl	INCX, X
	MOVDDUP( 0 * SIZE, X, %xmm2)
	MOVDDUP( 1 * SIZE, X, %xmm3)
	addl	INCX, X

	movsd	 0 * SIZE(Y), %xmm4
	movhpd	 1 * SIZE(Y), %xmm4
	addl	 INCY, Y
	movsd	 0 * SIZE(Y), %xmm5
	movhpd	 1 * SIZE(Y), %xmm5
	addl	 INCY, Y

	mulpd	ALPHA_R, %xmm0
	mulpd	ALPHA_R, %xmm2
	mulpd	ALPHA_I, %xmm1
	mulpd	ALPHA_I, %xmm3

	addpd	%xmm0, %xmm4
	addpd	%xmm2, %xmm5
	addpd	%xmm1, %xmm4
	addpd	%xmm3, %xmm5

	movlpd	%xmm4,   0 * SIZE(YY)
	movhpd	%xmm4,   1 * SIZE(YY)
	addl	 INCY, YY
	movlpd	%xmm5,   0 * SIZE(YY)
	movhpd	%xmm5,   1 * SIZE(YY)
	addl	 INCY, YY

	MOVDDUP( 0 * SIZE, X, %xmm0)
	MOVDDUP( 1 * SIZE, X, %xmm1)
	addl	INCX, X
	MOVDDUP( 0 * SIZE, X, %xmm2)
	MOVDDUP( 1 * SIZE, X, %xmm3)
	addl	INCX, X

	movsd	 0 * SIZE(Y), %xmm4
	movhpd	 1 * SIZE(Y), %xmm4
	addl	 INCY, Y
	movsd	 0 * SIZE(Y), %xmm5
	movhpd	 1 * SIZE(Y), %xmm5
	addl	 INCY, Y

	decl	%eax
	jg	.L51
	ALIGN_3

.L52:
	mulpd	ALPHA_R, %xmm0
	mulpd	ALPHA_R, %xmm2
	mulpd	ALPHA_I, %xmm1
	mulpd	ALPHA_I, %xmm3

	addpd	%xmm0, %xmm4
	addpd	%xmm2, %xmm5
	addpd	%xmm1, %xmm4
	addpd	%xmm3, %xmm5

	movlpd	%xmm4,   0 * SIZE(YY)
	movhpd	%xmm4,   1 * SIZE(YY)
	addl	 INCY, YY
	movlpd	%xmm5,   0 * SIZE(YY)
	movhpd	%xmm5,   1 * SIZE(YY)
	addl	 INCY, YY

	MOVDDUP( 0 * SIZE, X, %xmm0)
	MOVDDUP( 1 * SIZE, X, %xmm1)
	addl	INCX, X
	MOVDDUP( 0 * SIZE, X, %xmm2)
	MOVDDUP( 1 * SIZE, X, %xmm3)
	addl	INCX, X

	movsd	 0 * SIZE(Y), %xmm4
	movhpd	 1 * SIZE(Y), %xmm4
	addl	 INCY, Y
	movsd	 0 * SIZE(Y), %xmm5
	movhpd	 1 * SIZE(Y), %xmm5
	addl	 INCY, Y

	mulpd	ALPHA_R, %xmm0
	mulpd	ALPHA_R, %xmm2
	mulpd	ALPHA_I, %xmm1
	mulpd	ALPHA_I, %xmm3

	addpd	%xmm0, %xmm4
	addpd	%xmm2, %xmm5
	addpd	%xmm1, %xmm4
	addpd	%xmm3, %xmm5

	movlpd	%xmm4,   0 * SIZE(YY)
	movhpd	%xmm4,   1 * SIZE(YY)
	addl	 INCY, YY
	movlpd	%xmm5,   0 * SIZE(YY)
	movhpd	%xmm5,   1 * SIZE(YY)
	addl	 INCY, YY
	ALIGN_3

.L55:
	movl	M,  %eax
	andl	$2, %eax
	jle	.L57

	MOVDDUP( 0 * SIZE, X, %xmm0)
	MOVDDUP( 1 * SIZE, X, %xmm1)
	addl	INCX, X
	MOVDDUP( 0 * SIZE, X, %xmm2)
	MOVDDUP( 1 * SIZE, X, %xmm3)
	addl	INCX, X

	movsd	 0 * SIZE(Y), %xmm4
	movhpd	 1 * SIZE(Y), %xmm4
	addl	 INCY, Y
	movsd	 0 * SIZE(Y), %xmm5
	movhpd	 1 * SIZE(Y), %xmm5
	addl	 INCY, Y

	mulpd	ALPHA_R, %xmm0
	mulpd	ALPHA_R, %xmm2
	mulpd	ALPHA_I, %xmm1
	mulpd	ALPHA_I, %xmm3

	addpd	%xmm0, %xmm4
	addpd	%xmm2, %xmm5
	addpd	%xmm1, %xmm4
	addpd	%xmm3, %xmm5

	movlpd	%xmm4,   0 * SIZE(YY)
	movhpd	%xmm4,   1 * SIZE(YY)
	addl	 INCY, YY
	movlpd	%xmm5,   0 * SIZE(YY)
	movhpd	%xmm5,   1 * SIZE(YY)
	addl	 INCY, YY
	ALIGN_3

.L57:
	movl	M,  %eax
	andl	$1, %eax
	jle	.L999

.L58:
	MOVDDUP( 0 * SIZE, X, %xmm0)
	MOVDDUP( 1 * SIZE, X, %xmm1)

	movsd	 0 * SIZE(Y), %xmm4
	movhpd	 1 * SIZE(Y), %xmm4
	mulpd	ALPHA_R, %xmm0
	mulpd	ALPHA_I, %xmm1
	addpd	%xmm0, %xmm4
	addpd	%xmm1, %xmm4

	movlpd	%xmm4,   0 * SIZE(YY)
	movhpd	%xmm4,   1 * SIZE(YY)


	decl %eax
	jg	.L58
	ALIGN_3

.L999:
	popl	%ebx
	popl	%esi
	popl	%edi
	popl	%ebp
	ret

	EPILOGUE
