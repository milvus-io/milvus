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

#define STACK	12
#define ARGS     0

#define RESULT		 4 + STACK + ARGS(%esp)
#define STACK_N		 8 + STACK + ARGS(%esp)
#define STACK_X		12 + STACK + ARGS(%esp)
#define STACK_INCX	16 + STACK + ARGS(%esp)
#define STACK_Y		20 + STACK + ARGS(%esp)
#define STACK_INCY	24 + STACK + ARGS(%esp)

#define N	%ebx
#define X	%esi
#define INCX	%ecx
#define Y	%edi
#define INCY	%edx

#include "l1param.h"

#undef movsd

#ifndef OPTERON
#define MOVLPS	movsd
#else
#define MOVLPS	movlps
#endif

	PROLOGUE
	PROFCODE

	pushl	%edi
	pushl	%esi
	pushl	%ebx

	movl	STACK_N,     N
	movl	STACK_X,     X
	movl	STACK_INCX,  INCX
	movl	STACK_Y,     Y
	movl	STACK_INCY,  INCY

	sall	$ZBASE_SHIFT, INCX
	sall	$ZBASE_SHIFT, INCY

	xorps	%xmm0, %xmm0
	xorps	%xmm1, %xmm1

	cmpl	$0, N
	jle	.L999

	cmpl	$2 * SIZE, INCX
	jne	.L50
	cmpl	$2 * SIZE, INCY
	jne	.L50

	subl	$-16 * SIZE, X
	subl	$-16 * SIZE, Y

	testl	$SIZE, Y
	jne	.L30

	testl	$SIZE, X
	jne	.L20

	movl	N,  %eax
	sarl	$3, %eax
	jle	.L15

	movaps	-16 * SIZE(X), %xmm4
	movaps	-16 * SIZE(Y), %xmm6
	movaps	-14 * SIZE(X), %xmm5
	movaps	-14 * SIZE(Y), %xmm7

	decl	%eax
	jle	.L12
	ALIGN_3

.L11:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	pshufd	$0x4e, %xmm6, %xmm3
	mulpd	%xmm4, %xmm6
	addpd	%xmm6, %xmm0
	movaps	-12 * SIZE(Y), %xmm6
	mulpd	%xmm4, %xmm3
	movaps	-12 * SIZE(X), %xmm4
	addpd	%xmm3, %xmm1

	pshufd	$0x4e, %xmm7, %xmm3
	mulpd	%xmm5, %xmm7
	addpd	%xmm7, %xmm0
	movaps	-10 * SIZE(Y), %xmm7
	mulpd	%xmm5, %xmm3
	movaps	-10 * SIZE(X), %xmm5
	addpd	%xmm3, %xmm1

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	pshufd	$0x4e, %xmm6, %xmm3
	mulpd	%xmm4, %xmm6
	addpd	%xmm6, %xmm0
	movaps	 -8 * SIZE(Y), %xmm6
	mulpd	%xmm4, %xmm3
	movaps	 -8 * SIZE(X), %xmm4
	addpd	%xmm3, %xmm1

	pshufd	$0x4e, %xmm7, %xmm3
	mulpd	%xmm5, %xmm7
	addpd	%xmm7, %xmm0
	movaps	 -6 * SIZE(Y), %xmm7
	mulpd	%xmm5, %xmm3
	movaps	 -6 * SIZE(X), %xmm5
	addpd	%xmm3, %xmm1

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	pshufd	$0x4e, %xmm6, %xmm3
	mulpd	%xmm4, %xmm6
	addpd	%xmm6, %xmm0
	movaps	 -4 * SIZE(Y), %xmm6
	mulpd	%xmm4, %xmm3
	movaps	 -4 * SIZE(X), %xmm4
	addpd	%xmm3, %xmm1

	pshufd	$0x4e, %xmm7, %xmm3
	mulpd	%xmm5, %xmm7
	addpd	%xmm7, %xmm0
	movaps	 -2 * SIZE(Y), %xmm7
	mulpd	%xmm5, %xmm3
	movaps	 -2 * SIZE(X), %xmm5
	addpd	%xmm3, %xmm1

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	pshufd	$0x4e, %xmm6, %xmm3
	mulpd	%xmm4, %xmm6
	addpd	%xmm6, %xmm0
	movaps	  0 * SIZE(Y), %xmm6
	mulpd	%xmm4, %xmm3
	movaps	  0 * SIZE(X), %xmm4
	addpd	%xmm3, %xmm1

	pshufd	$0x4e, %xmm7, %xmm3
	mulpd	%xmm5, %xmm7
	addpd	%xmm7, %xmm0
	movaps	  2 * SIZE(Y), %xmm7
	mulpd	%xmm5, %xmm3
	movaps	  2 * SIZE(X), %xmm5
	addpd	%xmm3, %xmm1

	subl	$-16 * SIZE, X
	subl	$-16 * SIZE, Y

	decl	%eax
	jg	.L11
	ALIGN_3

.L12:
	pshufd	$0x4e, %xmm6, %xmm3
	mulpd	%xmm4, %xmm6
	addpd	%xmm6, %xmm0
	movaps	-12 * SIZE(Y), %xmm6
	mulpd	%xmm4, %xmm3
	movaps	-12 * SIZE(X), %xmm4
	addpd	%xmm3, %xmm1

	pshufd	$0x4e, %xmm7, %xmm3
	mulpd	%xmm5, %xmm7
	addpd	%xmm7, %xmm0
	movaps	-10 * SIZE(Y), %xmm7
	mulpd	%xmm5, %xmm3
	movaps	-10 * SIZE(X), %xmm5
	addpd	%xmm3, %xmm1

	pshufd	$0x4e, %xmm6, %xmm3
	mulpd	%xmm4, %xmm6
	addpd	%xmm6, %xmm0
	movaps	 -8 * SIZE(Y), %xmm6
	mulpd	%xmm4, %xmm3
	movaps	 -8 * SIZE(X), %xmm4
	addpd	%xmm3, %xmm1

	pshufd	$0x4e, %xmm7, %xmm3
	mulpd	%xmm5, %xmm7
	addpd	%xmm7, %xmm0
	movaps	 -6 * SIZE(Y), %xmm7
	mulpd	%xmm5, %xmm3
	movaps	 -6 * SIZE(X), %xmm5
	addpd	%xmm3, %xmm1

	pshufd	$0x4e, %xmm6, %xmm3
	mulpd	%xmm4, %xmm6
	addpd	%xmm6, %xmm0
	movaps	 -4 * SIZE(Y), %xmm6
	mulpd	%xmm4, %xmm3
	movaps	 -4 * SIZE(X), %xmm4
	addpd	%xmm3, %xmm1

	pshufd	$0x4e, %xmm7, %xmm3
	mulpd	%xmm5, %xmm7
	addpd	%xmm7, %xmm0
	movaps	 -2 * SIZE(Y), %xmm7
	mulpd	%xmm5, %xmm3
	movaps	 -2 * SIZE(X), %xmm5
	addpd	%xmm3, %xmm1

	pshufd	$0x4e, %xmm6, %xmm3
	mulpd	%xmm4, %xmm6
	addpd	%xmm6, %xmm0
	mulpd	%xmm4, %xmm3
	addpd	%xmm3, %xmm1

	pshufd	$0x4e, %xmm7, %xmm3
	mulpd	%xmm5, %xmm7
	addpd	%xmm7, %xmm0
	mulpd	%xmm5, %xmm3
	addpd	%xmm3, %xmm1

	subl	$-16 * SIZE, X
	subl	$-16 * SIZE, Y
	ALIGN_3

.L15:
	testl	$4, N
	jle	.L16

	movaps	-16 * SIZE(X), %xmm4
	movaps	-16 * SIZE(Y), %xmm6
	movaps	-14 * SIZE(X), %xmm5
	movaps	-14 * SIZE(Y), %xmm7

	pshufd	$0x4e, %xmm6, %xmm3
	mulpd	%xmm4, %xmm6
	addpd	%xmm6, %xmm0
	movaps	-12 * SIZE(Y), %xmm6
	mulpd	%xmm4, %xmm3
	movaps	-12 * SIZE(X), %xmm4
	addpd	%xmm3, %xmm1

	pshufd	$0x4e, %xmm7, %xmm3
	mulpd	%xmm5, %xmm7
	addpd	%xmm7, %xmm0
	movaps	-10 * SIZE(Y), %xmm7
	mulpd	%xmm5, %xmm3
	movaps	-10 * SIZE(X), %xmm5
	addpd	%xmm3, %xmm1

	pshufd	$0x4e, %xmm6, %xmm3
	mulpd	%xmm4, %xmm6
	addpd	%xmm6, %xmm0
	mulpd	%xmm4, %xmm3
	addpd	%xmm3, %xmm1

	pshufd	$0x4e, %xmm7, %xmm3
	mulpd	%xmm5, %xmm7
	addpd	%xmm7, %xmm0
	mulpd	%xmm5, %xmm3
	addpd	%xmm3, %xmm1

	addl	$8 * SIZE, X
	addl	$8 * SIZE, Y
	ALIGN_3

.L16:
	testl	$2, N
	jle	.L17

	movaps	-16 * SIZE(X), %xmm4
	movaps	-16 * SIZE(Y), %xmm6
	movaps	-14 * SIZE(X), %xmm5
	movaps	-14 * SIZE(Y), %xmm7

	pshufd	$0x4e, %xmm6, %xmm3
	mulpd	%xmm4, %xmm6
	addpd	%xmm6, %xmm0
	mulpd	%xmm4, %xmm3
	addpd	%xmm3, %xmm1

	pshufd	$0x4e, %xmm7, %xmm3
	mulpd	%xmm5, %xmm7
	addpd	%xmm7, %xmm0
	mulpd	%xmm5, %xmm3
	addpd	%xmm3, %xmm1

	addl	$4 * SIZE, X
	addl	$4 * SIZE, Y
	ALIGN_3

.L17:
	testl	$1, N
	jle	.L98

	movaps	-16 * SIZE(X), %xmm4
	movaps	-16 * SIZE(Y), %xmm6

	pshufd	$0x4e,  %xmm6, %xmm3
	mulpd	%xmm4,  %xmm6
	addpd	%xmm6,  %xmm0
	mulpd	%xmm4,  %xmm3
	addpd	%xmm3, %xmm1
	jmp	.L98
	ALIGN_3

.L20:
	movl	N,  %eax
	sarl	$3, %eax
	jle	.L25

	MOVLPS	-16 * SIZE(X), %xmm4
	movhps	-15 * SIZE(X), %xmm4
	movaps	-16 * SIZE(Y), %xmm6

	MOVLPS	-14 * SIZE(X), %xmm5
	movhps	-13 * SIZE(X), %xmm5
	movaps	-14 * SIZE(Y), %xmm7

	decl	%eax
	jle	.L22
	ALIGN_3

.L21:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	pshufd	$0x4e, %xmm6, %xmm3
	mulpd	%xmm4, %xmm6
	addpd	%xmm6, %xmm0
	movaps	-12 * SIZE(Y), %xmm6
	mulpd	%xmm4, %xmm3
	MOVLPS	-12 * SIZE(X), %xmm4
	movhps	-11 * SIZE(X), %xmm4
	addpd	%xmm3, %xmm1

	pshufd	$0x4e, %xmm7, %xmm3
	mulpd	%xmm5, %xmm7
	addpd	%xmm7, %xmm0
	movaps	-10 * SIZE(Y), %xmm7
	mulpd	%xmm5, %xmm3
	MOVLPS	-10 * SIZE(X), %xmm5
	movhps	 -9 * SIZE(X), %xmm5
	addpd	%xmm3, %xmm1

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	pshufd	$0x4e, %xmm6, %xmm3
	mulpd	%xmm4, %xmm6
	addpd	%xmm6, %xmm0
	movaps	 -8 * SIZE(Y), %xmm6
	mulpd	%xmm4, %xmm3
	MOVLPS	 -8 * SIZE(X), %xmm4
	movhps	 -7 * SIZE(X), %xmm4
	addpd	%xmm3, %xmm1

	pshufd	$0x4e, %xmm7, %xmm3
	mulpd	%xmm5, %xmm7
	addpd	%xmm7, %xmm0
	movaps	 -6 * SIZE(Y), %xmm7
	mulpd	%xmm5, %xmm3
	MOVLPS	 -6 * SIZE(X), %xmm5
	movhps	 -5 * SIZE(X), %xmm5
	addpd	%xmm3, %xmm1

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	pshufd	$0x4e, %xmm6, %xmm3
	mulpd	%xmm4, %xmm6
	addpd	%xmm6, %xmm0
	movaps	 -4 * SIZE(Y), %xmm6
	mulpd	%xmm4, %xmm3
	MOVLPS	 -4 * SIZE(X), %xmm4
	movhps	 -3 * SIZE(X), %xmm4
	addpd	%xmm3, %xmm1

	pshufd	$0x4e, %xmm7, %xmm3
	mulpd	%xmm5, %xmm7
	addpd	%xmm7, %xmm0
	movaps	 -2 * SIZE(Y), %xmm7
	mulpd	%xmm5, %xmm3
	MOVLPS	 -2 * SIZE(X), %xmm5
	movhps	 -1 * SIZE(X), %xmm5
	addpd	%xmm3, %xmm1

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	pshufd	$0x4e, %xmm6, %xmm3
	mulpd	%xmm4, %xmm6
	addpd	%xmm6, %xmm0
	movaps	  0 * SIZE(Y), %xmm6
	mulpd	%xmm4, %xmm3
	MOVLPS	  0 * SIZE(X), %xmm4
	movhps	  1 * SIZE(X), %xmm4
	addpd	%xmm3, %xmm1

	pshufd	$0x4e, %xmm7, %xmm3
	mulpd	%xmm5, %xmm7
	addpd	%xmm7, %xmm0
	movaps	  2 * SIZE(Y), %xmm7
	mulpd	%xmm5, %xmm3
	MOVLPS	  2 * SIZE(X), %xmm5
	movhps	  3 * SIZE(X), %xmm5
	addpd	%xmm3, %xmm1

	subl	$-16 * SIZE, X
	subl	$-16 * SIZE, Y

	decl	%eax
	jg	.L21
	ALIGN_3

.L22:
	pshufd	$0x4e, %xmm6, %xmm3
	mulpd	%xmm4, %xmm6
	addpd	%xmm6, %xmm0
	movaps	-12 * SIZE(Y), %xmm6
	mulpd	%xmm4, %xmm3
	MOVLPS	-12 * SIZE(X), %xmm4
	movhps	-11 * SIZE(X), %xmm4
	addpd	%xmm3, %xmm1

	pshufd	$0x4e, %xmm7, %xmm3
	mulpd	%xmm5, %xmm7
	addpd	%xmm7, %xmm0
	movaps	-10 * SIZE(Y), %xmm7
	mulpd	%xmm5, %xmm3
	MOVLPS	-10 * SIZE(X), %xmm5
	movhps	 -9 * SIZE(X), %xmm5
	addpd	%xmm3, %xmm1

	pshufd	$0x4e, %xmm6, %xmm3
	mulpd	%xmm4, %xmm6
	addpd	%xmm6, %xmm0
	movaps	 -8 * SIZE(Y), %xmm6
	mulpd	%xmm4, %xmm3
	MOVLPS	 -8 * SIZE(X), %xmm4
	movhps	 -7 * SIZE(X), %xmm4
	addpd	%xmm3, %xmm1

	pshufd	$0x4e, %xmm7, %xmm3
	mulpd	%xmm5, %xmm7
	addpd	%xmm7, %xmm0
	movaps	 -6 * SIZE(Y), %xmm7
	mulpd	%xmm5, %xmm3
	MOVLPS	 -6 * SIZE(X), %xmm5
	movhps	 -5 * SIZE(X), %xmm5
	addpd	%xmm3, %xmm1

	pshufd	$0x4e, %xmm6, %xmm3
	mulpd	%xmm4, %xmm6
	addpd	%xmm6, %xmm0
	movaps	 -4 * SIZE(Y), %xmm6
	mulpd	%xmm4, %xmm3
	MOVLPS	 -4 * SIZE(X), %xmm4
	movhps	 -3 * SIZE(X), %xmm4
	addpd	%xmm3, %xmm1

	pshufd	$0x4e, %xmm7, %xmm3
	mulpd	%xmm5, %xmm7
	addpd	%xmm7, %xmm0
	movaps	 -2 * SIZE(Y), %xmm7
	mulpd	%xmm5, %xmm3
	MOVLPS	 -2 * SIZE(X), %xmm5
	movhps	 -1 * SIZE(X), %xmm5
	addpd	%xmm3, %xmm1

	pshufd	$0x4e, %xmm6, %xmm3
	mulpd	%xmm4, %xmm6
	addpd	%xmm6, %xmm0
	mulpd	%xmm4, %xmm3
	addpd	%xmm3, %xmm1

	pshufd	$0x4e, %xmm7, %xmm3
	mulpd	%xmm5, %xmm7
	addpd	%xmm7, %xmm0
	mulpd	%xmm5, %xmm3
	addpd	%xmm3, %xmm1

	subl	$-16 * SIZE, X
	subl	$-16 * SIZE, Y
	ALIGN_3

.L25:
	testl	$4, N
	jle	.L26

	MOVLPS	-16 * SIZE(X), %xmm4
	movhps	-15 * SIZE(X), %xmm4
	movaps	-16 * SIZE(Y), %xmm6

	MOVLPS	-14 * SIZE(X), %xmm5
	movhps	-13 * SIZE(X), %xmm5
	movaps	-14 * SIZE(Y), %xmm7

	pshufd	$0x4e, %xmm6, %xmm3
	mulpd	%xmm4, %xmm6
	addpd	%xmm6, %xmm0
	movaps	-12 * SIZE(Y), %xmm6
	mulpd	%xmm4, %xmm3
	MOVLPS	-12 * SIZE(X), %xmm4
	movhps	-11 * SIZE(X), %xmm4
	addpd	%xmm3, %xmm1

	pshufd	$0x4e, %xmm7, %xmm3
	mulpd	%xmm5, %xmm7
	addpd	%xmm7, %xmm0
	movaps	-10 * SIZE(Y), %xmm7
	mulpd	%xmm5, %xmm3
	MOVLPS	-10 * SIZE(X), %xmm5
	movhps	 -9 * SIZE(X), %xmm5
	addpd	%xmm3, %xmm1

	pshufd	$0x4e, %xmm6, %xmm3
	mulpd	%xmm4, %xmm6
	addpd	%xmm6, %xmm0
	mulpd	%xmm4, %xmm3
	addpd	%xmm3, %xmm1

	pshufd	$0x4e, %xmm7, %xmm3
	mulpd	%xmm5, %xmm7
	addpd	%xmm7, %xmm0
	mulpd	%xmm5, %xmm3
	addpd	%xmm3, %xmm1

	addl	$8 * SIZE, X
	addl	$8 * SIZE, Y
	ALIGN_3

.L26:
	testl	$2, N
	jle	.L27

	MOVLPS	-16 * SIZE(X), %xmm4
	movhps	-15 * SIZE(X), %xmm4
	movaps	-16 * SIZE(Y), %xmm6

	pshufd	$0x4e, %xmm6, %xmm3
	mulpd	%xmm4, %xmm6
	addpd	%xmm6, %xmm0
	mulpd	%xmm4, %xmm3
	addpd	%xmm3, %xmm1

	MOVLPS	-14 * SIZE(X), %xmm5
	movhps	-13 * SIZE(X), %xmm5
	movaps	-14 * SIZE(Y), %xmm7

	pshufd	$0x4e, %xmm7, %xmm3
	mulpd	%xmm5, %xmm7
	addpd	%xmm7, %xmm0
	mulpd	%xmm5, %xmm3
	addpd	%xmm3, %xmm1

	addl	$4 * SIZE, X
	addl	$4 * SIZE, Y
	ALIGN_3

.L27:
	testl	$1, N
	jle	.L98

	MOVLPS	-16 * SIZE(X), %xmm4
	movhps	-15 * SIZE(X), %xmm4
	movaps	-16 * SIZE(Y), %xmm6

	pshufd	$0x4e,  %xmm6, %xmm3
	mulpd	%xmm4,  %xmm6
	addpd	%xmm6,  %xmm0
	mulpd	%xmm4,  %xmm3
	addpd	%xmm3, %xmm1
	jmp	.L98
	ALIGN_3

.L30:
	testl	$SIZE, X
	jne	.L40

	movl	N,  %eax
	sarl	$3, %eax
	jle	.L35

	MOVLPS	-16 * SIZE(Y), %xmm4
	movhps	-15 * SIZE(Y), %xmm4
	movaps	-16 * SIZE(X), %xmm6

	MOVLPS	-14 * SIZE(Y), %xmm5
	movhps	-13 * SIZE(Y), %xmm5
	movaps	-14 * SIZE(X), %xmm7

	decl	%eax
	jle	.L32
	ALIGN_3

.L31:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	pshufd	$0x4e, %xmm6, %xmm3
	mulpd	%xmm4, %xmm6
	addpd	%xmm6, %xmm0
	movaps	-12 * SIZE(X), %xmm6
	mulpd	%xmm4, %xmm3
	MOVLPS	-12 * SIZE(Y), %xmm4
	movhps	-11 * SIZE(Y), %xmm4
	addpd	%xmm3, %xmm1

	pshufd	$0x4e, %xmm7, %xmm3
	mulpd	%xmm5, %xmm7
	addpd	%xmm7, %xmm0
	movaps	-10 * SIZE(X), %xmm7
	mulpd	%xmm5, %xmm3
	MOVLPS	-10 * SIZE(Y), %xmm5
	movhps	 -9 * SIZE(Y), %xmm5
	addpd	%xmm3, %xmm1

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif
	pshufd	$0x4e, %xmm6, %xmm3
	mulpd	%xmm4, %xmm6
	addpd	%xmm6, %xmm0
	movaps	 -8 * SIZE(X), %xmm6
	mulpd	%xmm4, %xmm3
	MOVLPS	 -8 * SIZE(Y), %xmm4
	movhps	 -7 * SIZE(Y), %xmm4
	addpd	%xmm3, %xmm1

	pshufd	$0x4e, %xmm7, %xmm3
	mulpd	%xmm5, %xmm7
	addpd	%xmm7, %xmm0
	movaps	 -6 * SIZE(X), %xmm7
	mulpd	%xmm5, %xmm3
	MOVLPS	 -6 * SIZE(Y), %xmm5
	movhps	 -5 * SIZE(Y), %xmm5
	addpd	%xmm3, %xmm1

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	pshufd	$0x4e, %xmm6, %xmm3
	mulpd	%xmm4, %xmm6
	addpd	%xmm6, %xmm0
	movaps	 -4 * SIZE(X), %xmm6
	mulpd	%xmm4, %xmm3
	MOVLPS	 -4 * SIZE(Y), %xmm4
	movhps	 -3 * SIZE(Y), %xmm4
	addpd	%xmm3, %xmm1

	pshufd	$0x4e, %xmm7, %xmm3
	mulpd	%xmm5, %xmm7
	addpd	%xmm7, %xmm0
	movaps	 -2 * SIZE(X), %xmm7
	mulpd	%xmm5, %xmm3
	MOVLPS	 -2 * SIZE(Y), %xmm5
	movhps	 -1 * SIZE(Y), %xmm5
	addpd	%xmm3, %xmm1

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	pshufd	$0x4e, %xmm6, %xmm3
	mulpd	%xmm4, %xmm6
	addpd	%xmm6, %xmm0
	movaps	  0 * SIZE(X), %xmm6
	mulpd	%xmm4, %xmm3
	MOVLPS	  0 * SIZE(Y), %xmm4
	movhps	  1 * SIZE(Y), %xmm4
	addpd	%xmm3, %xmm1

	pshufd	$0x4e, %xmm7, %xmm3
	mulpd	%xmm5, %xmm7
	addpd	%xmm7, %xmm0
	movaps	  2 * SIZE(X), %xmm7
	mulpd	%xmm5, %xmm3
	MOVLPS	  2 * SIZE(Y), %xmm5
	movhps	  3 * SIZE(Y), %xmm5
	addpd	%xmm3, %xmm1

	subl	$-16 * SIZE, X
	subl	$-16 * SIZE, Y

	decl	%eax
	jg	.L31
	ALIGN_3

.L32:
	pshufd	$0x4e, %xmm6, %xmm3
	mulpd	%xmm4, %xmm6
	addpd	%xmm6, %xmm0
	movaps	-12 * SIZE(X), %xmm6
	mulpd	%xmm4, %xmm3
	MOVLPS	-12 * SIZE(Y), %xmm4
	movhps	-11 * SIZE(Y), %xmm4
	addpd	%xmm3, %xmm1

	pshufd	$0x4e, %xmm7, %xmm3
	mulpd	%xmm5, %xmm7
	addpd	%xmm7, %xmm0
	movaps	-10 * SIZE(X), %xmm7
	mulpd	%xmm5, %xmm3
	MOVLPS	-10 * SIZE(Y), %xmm5
	movhps	 -9 * SIZE(Y), %xmm5
	addpd	%xmm3, %xmm1

	pshufd	$0x4e, %xmm6, %xmm3
	mulpd	%xmm4, %xmm6
	addpd	%xmm6, %xmm0
	movaps	 -8 * SIZE(X), %xmm6
	mulpd	%xmm4, %xmm3
	MOVLPS	 -8 * SIZE(Y), %xmm4
	movhps	 -7 * SIZE(Y), %xmm4
	addpd	%xmm3, %xmm1

	pshufd	$0x4e, %xmm7, %xmm3
	mulpd	%xmm5, %xmm7
	addpd	%xmm7, %xmm0
	movaps	 -6 * SIZE(X), %xmm7
	mulpd	%xmm5, %xmm3
	MOVLPS	 -6 * SIZE(Y), %xmm5
	movhps	 -5 * SIZE(Y), %xmm5
	addpd	%xmm3, %xmm1

	pshufd	$0x4e, %xmm6, %xmm3
	mulpd	%xmm4, %xmm6
	addpd	%xmm6, %xmm0
	movaps	 -4 * SIZE(X), %xmm6
	mulpd	%xmm4, %xmm3
	MOVLPS	 -4 * SIZE(Y), %xmm4
	movhps	 -3 * SIZE(Y), %xmm4
	addpd	%xmm3, %xmm1

	pshufd	$0x4e, %xmm7, %xmm3
	mulpd	%xmm5, %xmm7
	addpd	%xmm7, %xmm0
	movaps	 -2 * SIZE(X), %xmm7
	mulpd	%xmm5, %xmm3
	MOVLPS	 -2 * SIZE(Y), %xmm5
	movhps	 -1 * SIZE(Y), %xmm5
	addpd	%xmm3, %xmm1

	pshufd	$0x4e, %xmm6, %xmm3
	mulpd	%xmm4, %xmm6
	addpd	%xmm6, %xmm0
	mulpd	%xmm4, %xmm3
	addpd	%xmm3, %xmm1

	pshufd	$0x4e, %xmm7, %xmm3
	mulpd	%xmm5, %xmm7
	addpd	%xmm7, %xmm0
	mulpd	%xmm5, %xmm3
	addpd	%xmm3, %xmm1

	subl	$-16 * SIZE, X
	subl	$-16 * SIZE, Y
	ALIGN_3

.L35:
	testl	$4, N
	jle	.L36

	MOVLPS	-16 * SIZE(Y), %xmm4
	movhps	-15 * SIZE(Y), %xmm4
	movaps	-16 * SIZE(X), %xmm6

	MOVLPS	-14 * SIZE(Y), %xmm5
	movhps	-13 * SIZE(Y), %xmm5
	movaps	-14 * SIZE(X), %xmm7

	pshufd	$0x4e, %xmm6, %xmm3
	mulpd	%xmm4, %xmm6
	addpd	%xmm6, %xmm0
	movaps	-12 * SIZE(X), %xmm6
	mulpd	%xmm4, %xmm3
	MOVLPS	-12 * SIZE(Y), %xmm4
	movhps	-11 * SIZE(Y), %xmm4
	addpd	%xmm3, %xmm1

	pshufd	$0x4e, %xmm7, %xmm3
	mulpd	%xmm5, %xmm7
	addpd	%xmm7, %xmm0
	movaps	-10 * SIZE(X), %xmm7
	mulpd	%xmm5, %xmm3
	MOVLPS	-10 * SIZE(Y), %xmm5
	movhps	 -9 * SIZE(Y), %xmm5
	addpd	%xmm3, %xmm1

	pshufd	$0x4e, %xmm6, %xmm3
	mulpd	%xmm4, %xmm6
	addpd	%xmm6, %xmm0
	mulpd	%xmm4, %xmm3
	addpd	%xmm3, %xmm1

	pshufd	$0x4e, %xmm7, %xmm3
	mulpd	%xmm5, %xmm7
	addpd	%xmm7, %xmm0
	mulpd	%xmm5, %xmm3
	addpd	%xmm3, %xmm1

	addl	$8 * SIZE, X
	addl	$8 * SIZE, Y
	ALIGN_3

.L36:
	testl	$2, N
	jle	.L37

	MOVLPS	-16 * SIZE(Y), %xmm4
	movhps	-15 * SIZE(Y), %xmm4
	movaps	-16 * SIZE(X), %xmm6

	pshufd	$0x4e, %xmm6, %xmm3
	mulpd	%xmm4, %xmm6
	addpd	%xmm6, %xmm0
	mulpd	%xmm4, %xmm3
	addpd	%xmm3, %xmm1

	MOVLPS	-14 * SIZE(Y), %xmm5
	movhps	-13 * SIZE(Y), %xmm5
	movaps	-14 * SIZE(X), %xmm7

	pshufd	$0x4e, %xmm7, %xmm3
	mulpd	%xmm5, %xmm7
	addpd	%xmm7, %xmm0
	mulpd	%xmm5, %xmm3
	addpd	%xmm3, %xmm1

	addl	$4 * SIZE, X
	addl	$4 * SIZE, Y
	ALIGN_3

.L37:
	SHUFPD_1 %xmm1, %xmm1
	SHUFPD_1 %xmm3, %xmm3

	testl	$1, N
	jle	.L98

	MOVLPS	-16 * SIZE(Y), %xmm4
	movhps	-15 * SIZE(Y), %xmm4
	movaps	-16 * SIZE(X), %xmm6

	pshufd	$0x4e,  %xmm6, %xmm3
	mulpd	%xmm4,  %xmm6
	addpd	%xmm6,  %xmm0
	mulpd	%xmm4,  %xmm3
	SHUFPD_1 %xmm3, %xmm3
	addpd	%xmm3, %xmm1
	jmp	.L98
	ALIGN_3

.L40:
	movhps	-16 * SIZE(X), %xmm4
	addl	$SIZE, X
	movhps	-16 * SIZE(Y), %xmm6
	addl	$SIZE, Y

	movl	N,  %eax
	sarl	$3, %eax
	jle	.L45

	movaps	-16 * SIZE(X), %xmm5
	movaps	-16 * SIZE(Y), %xmm7

	decl	%eax
	jle	.L42
	ALIGN_3

.L41:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif


#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif


#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	movsd	%xmm7, %xmm6
	pshufd	$0x4e, %xmm6, %xmm3
	movsd	%xmm5, %xmm4
	mulpd	%xmm4, %xmm6
	addpd	%xmm6, %xmm0
	movaps	-14 * SIZE(Y), %xmm6
	mulpd	%xmm4, %xmm3
	movaps	-14 * SIZE(X), %xmm4
	addpd	%xmm3, %xmm1

	movsd	%xmm6, %xmm7
	pshufd	$0x4e,  %xmm7, %xmm3
	movsd	%xmm4,  %xmm5
	mulpd	%xmm5,  %xmm7
	addpd	%xmm7,  %xmm0
	movaps	-12 * SIZE(Y), %xmm7
	mulpd	%xmm5,  %xmm3
	movaps	-12 * SIZE(X), %xmm5
	addpd	%xmm3, %xmm1

	movsd	%xmm7, %xmm6
	pshufd	$0x4e, %xmm6, %xmm3
	movsd	%xmm5, %xmm4
	mulpd	%xmm4, %xmm6
	addpd	%xmm6, %xmm0
	movaps	-10 * SIZE(Y), %xmm6
	mulpd	%xmm4, %xmm3
	movaps	-10 * SIZE(X), %xmm4
	addpd	%xmm3, %xmm1

	movsd	%xmm6, %xmm7
	pshufd	$0x4e,  %xmm7, %xmm3
	movsd	%xmm4,  %xmm5
	mulpd	%xmm5,  %xmm7
	addpd	%xmm7,  %xmm0
	movaps	 -8 * SIZE(Y), %xmm7
	mulpd	%xmm5,  %xmm3
	movaps	 -8 * SIZE(X), %xmm5
	addpd	%xmm3, %xmm1

	movsd	%xmm7, %xmm6
	pshufd	$0x4e, %xmm6, %xmm3
	movsd	%xmm5, %xmm4
	mulpd	%xmm4, %xmm6
	addpd	%xmm6, %xmm0
	movaps	 -6 * SIZE(Y), %xmm6
	mulpd	%xmm4, %xmm3
	movaps	 -6 * SIZE(X), %xmm4
	addpd	%xmm3, %xmm1

	movsd	%xmm6, %xmm7
	pshufd	$0x4e,  %xmm7, %xmm3
	movsd	%xmm4,  %xmm5
	mulpd	%xmm5,  %xmm7
	addpd	%xmm7,  %xmm0
	movaps	 -4 * SIZE(Y), %xmm7
	mulpd	%xmm5,  %xmm3
	movaps	 -4 * SIZE(X), %xmm5
	addpd	%xmm3, %xmm1

	movsd	%xmm7, %xmm6
	pshufd	$0x4e, %xmm6, %xmm3
	movsd	%xmm5, %xmm4
	mulpd	%xmm4, %xmm6
	addpd	%xmm6, %xmm0
	movaps	 -2 * SIZE(Y), %xmm6
	mulpd	%xmm4, %xmm3
	movaps	 -2 * SIZE(X), %xmm4
	addpd	%xmm3, %xmm1

	movsd	%xmm6, %xmm7
	pshufd	$0x4e,  %xmm7, %xmm3
	movsd	%xmm4,  %xmm5
	mulpd	%xmm5,  %xmm7
	addpd	%xmm7,  %xmm0
	movaps	  0 * SIZE(Y), %xmm7
	mulpd	%xmm5,  %xmm3
	movaps	  0 * SIZE(X), %xmm5
	addpd	%xmm3, %xmm1

	subl	$-16 * SIZE, X
	subl	$-16 * SIZE, Y

	decl	%eax
	jg	.L41
	ALIGN_3

.L42:
	movsd	%xmm7, %xmm6
	pshufd	$0x4e, %xmm6, %xmm3
	movsd	%xmm5, %xmm4
	mulpd	%xmm4, %xmm6
	addpd	%xmm6, %xmm0
	movaps	-14 * SIZE(Y), %xmm6
	mulpd	%xmm4, %xmm3
	movaps	-14 * SIZE(X), %xmm4
	addpd	%xmm3, %xmm1

	movsd	%xmm6, %xmm7
	pshufd	$0x4e,  %xmm7, %xmm3
	movsd	%xmm4,  %xmm5
	mulpd	%xmm5,  %xmm7
	addpd	%xmm7,  %xmm0
	movaps	-12 * SIZE(Y), %xmm7
	mulpd	%xmm5,  %xmm3
	movaps	-12 * SIZE(X), %xmm5
	addpd	%xmm3, %xmm1

	movsd	%xmm7, %xmm6
	pshufd	$0x4e, %xmm6, %xmm3
	movsd	%xmm5, %xmm4
	mulpd	%xmm4, %xmm6
	addpd	%xmm6, %xmm0
	movaps	-10 * SIZE(Y), %xmm6
	mulpd	%xmm4, %xmm3
	movaps	-10 * SIZE(X), %xmm4
	addpd	%xmm3, %xmm1

	movsd	%xmm6, %xmm7
	pshufd	$0x4e,  %xmm7, %xmm3
	movsd	%xmm4,  %xmm5
	mulpd	%xmm5,  %xmm7
	addpd	%xmm7,  %xmm0
	movaps	 -8 * SIZE(Y), %xmm7
	mulpd	%xmm5,  %xmm3
	movaps	 -8 * SIZE(X), %xmm5
	addpd	%xmm3, %xmm1

	movsd	%xmm7, %xmm6
	pshufd	$0x4e, %xmm6, %xmm3
	movsd	%xmm5, %xmm4
	mulpd	%xmm4, %xmm6
	addpd	%xmm6, %xmm0
	movaps	 -6 * SIZE(Y), %xmm6
	mulpd	%xmm4, %xmm3
	movaps	 -6 * SIZE(X), %xmm4
	addpd	%xmm3, %xmm1

	movsd	%xmm6, %xmm7
	pshufd	$0x4e,  %xmm7, %xmm3
	movsd	%xmm4,  %xmm5
	mulpd	%xmm5,  %xmm7
	addpd	%xmm7,  %xmm0
	movaps	 -4 * SIZE(Y), %xmm7
	mulpd	%xmm5,  %xmm3
	movaps	 -4 * SIZE(X), %xmm5
	addpd	%xmm3, %xmm1

	movsd	%xmm7, %xmm6
	pshufd	$0x4e, %xmm6, %xmm3
	movsd	%xmm5, %xmm4
	mulpd	%xmm4, %xmm6
	addpd	%xmm6, %xmm0
	movaps	 -2 * SIZE(Y), %xmm6
	mulpd	%xmm4, %xmm3
	movaps	 -2 * SIZE(X), %xmm4
	addpd	%xmm3, %xmm1

	movsd	%xmm6, %xmm7
	pshufd	$0x4e,  %xmm7, %xmm3
	movsd	%xmm4,  %xmm5
	mulpd	%xmm5,  %xmm7
	addpd	%xmm7,  %xmm0
	mulpd	%xmm5,  %xmm3
	addpd	%xmm3, %xmm1

	subl	$-16 * SIZE, X
	subl	$-16 * SIZE, Y
	ALIGN_3

.L45:
	testl	$4, N
	jle	.L46

	movaps	-16 * SIZE(X), %xmm5
	movaps	-16 * SIZE(Y), %xmm7

	movsd	%xmm7, %xmm6
	pshufd	$0x4e, %xmm6, %xmm3
	movsd	%xmm5, %xmm4
	mulpd	%xmm4, %xmm6
	addpd	%xmm6, %xmm0
	movaps	-14 * SIZE(Y), %xmm6
	mulpd	%xmm4, %xmm3
	movaps	-14 * SIZE(X), %xmm4
	addpd	%xmm3, %xmm1

	movsd	%xmm6, %xmm7
	pshufd	$0x4e,  %xmm7, %xmm3
	movsd	%xmm4,  %xmm5
	mulpd	%xmm5,  %xmm7
	addpd	%xmm7,  %xmm0
	movaps	-12 * SIZE(Y), %xmm7
	mulpd	%xmm5,  %xmm3
	movaps	-12 * SIZE(X), %xmm5
	addpd	%xmm3, %xmm1

	movsd	%xmm7, %xmm6
	pshufd	$0x4e, %xmm6, %xmm3
	movsd	%xmm5, %xmm4
	mulpd	%xmm4, %xmm6
	addpd	%xmm6, %xmm0
	movaps	-10 * SIZE(Y), %xmm6
	mulpd	%xmm4, %xmm3
	movaps	-10 * SIZE(X), %xmm4
	addpd	%xmm3, %xmm1

	movsd	%xmm6, %xmm7
	pshufd	$0x4e,  %xmm7, %xmm3
	movsd	%xmm4,  %xmm5
	mulpd	%xmm5,  %xmm7
	addpd	%xmm7,  %xmm0
	mulpd	%xmm5,  %xmm3
	addpd	%xmm3, %xmm1

	addl	$8 * SIZE, X
	addl	$8 * SIZE, Y
	ALIGN_3

.L46:
	testl	$2, N
	jle	.L47

	movaps	-16 * SIZE(X), %xmm5
	movaps	-16 * SIZE(Y), %xmm7

	movsd	%xmm7, %xmm6
	pshufd	$0x4e, %xmm6, %xmm3
	movsd	%xmm5, %xmm4
	mulpd	%xmm4, %xmm6
	addpd	%xmm6, %xmm0
	movaps	-14 * SIZE(Y), %xmm6
	mulpd	%xmm4, %xmm3
	movaps	-14 * SIZE(X), %xmm4
	addpd	%xmm3, %xmm1

	movsd	%xmm6, %xmm7
	pshufd	$0x4e,  %xmm7, %xmm3
	movsd	%xmm4,  %xmm5
	mulpd	%xmm5,  %xmm7
	addpd	%xmm7,  %xmm0
	mulpd	%xmm5,  %xmm3
	addpd	%xmm3, %xmm1

	addl	$4 * SIZE, X
	addl	$4 * SIZE, Y
	ALIGN_3

.L47:
	testl	$1, N
	jle	.L48

	movlpd	-16 * SIZE(X), %xmm4
	movlpd	-16 * SIZE(Y), %xmm6

	pshufd	$0x4e,  %xmm6, %xmm3
	mulpd	%xmm4,  %xmm6
	addpd	%xmm6,  %xmm0
	mulpd	%xmm4,  %xmm3
	addpd	%xmm3, %xmm1
	ALIGN_3

.L48:
	SHUFPD_1 %xmm0, %xmm0
	SHUFPD_1 %xmm1, %xmm1
	SHUFPD_1 %xmm2, %xmm2
	SHUFPD_1 %xmm3, %xmm3
	jmp	.L98
	ALIGN_3

.L50:
	movl	N,  %eax
	sarl	$3, %eax
	jle	.L55

	MOVLPS	0 * SIZE(X), %xmm4
	movhps	1 * SIZE(X), %xmm4
	addl	INCX, X
	MOVLPS	0 * SIZE(Y), %xmm6
	movhps	1 * SIZE(Y), %xmm6
	addl	INCY, Y

	MOVLPS	0 * SIZE(X), %xmm5
	movhps	1 * SIZE(X), %xmm5
	addl	INCX, X
	MOVLPS	0 * SIZE(Y), %xmm7
	movhps	1 * SIZE(Y), %xmm7
	addl	INCY, Y

	decl	%eax
	jle	.L54
	ALIGN_3

.L53:
	pshufd	$0x4e, %xmm6, %xmm3
	mulpd	%xmm4, %xmm6
	addpd	%xmm6, %xmm0
	MOVLPS	0 * SIZE(Y), %xmm6
	movhps	1 * SIZE(Y), %xmm6
	addl	INCY, Y
	mulpd	%xmm4, %xmm3
	MOVLPS	0 * SIZE(X), %xmm4
	movhps	1 * SIZE(X), %xmm4
	addl	INCX, X
	addpd	%xmm3, %xmm1

	pshufd	$0x4e, %xmm7, %xmm3
	mulpd	%xmm5, %xmm7
	addpd	%xmm7, %xmm0
	MOVLPS	0 * SIZE(Y), %xmm7
	movhps	1 * SIZE(Y), %xmm7
	addl	INCY, Y
	mulpd	%xmm5, %xmm3
	MOVLPS	0 * SIZE(X), %xmm5
	movhps	1 * SIZE(X), %xmm5
	addl	INCX, X
	addpd	%xmm3, %xmm1

	pshufd	$0x4e, %xmm6, %xmm3
	mulpd	%xmm4, %xmm6
	addpd	%xmm6, %xmm0
	MOVLPS	0 * SIZE(Y), %xmm6
	movhps	1 * SIZE(Y), %xmm6
	addl	INCY, Y
	mulpd	%xmm4, %xmm3
	MOVLPS	0 * SIZE(X), %xmm4
	movhps	1 * SIZE(X), %xmm4
	addl	INCX, X
	addpd	%xmm3, %xmm1

	pshufd	$0x4e, %xmm7, %xmm3
	mulpd	%xmm5, %xmm7
	addpd	%xmm7, %xmm0
	MOVLPS	0 * SIZE(Y), %xmm7
	movhps	1 * SIZE(Y), %xmm7
	addl	INCY, Y
	mulpd	%xmm5, %xmm3
	MOVLPS	0 * SIZE(X), %xmm5
	movhps	1 * SIZE(X), %xmm5
	addl	INCX, X
	addpd	%xmm3, %xmm1

	pshufd	$0x4e, %xmm6, %xmm3
	mulpd	%xmm4, %xmm6
	addpd	%xmm6, %xmm0
	MOVLPS	0 * SIZE(Y), %xmm6
	movhps	1 * SIZE(Y), %xmm6
	addl	INCY, Y
	mulpd	%xmm4, %xmm3
	MOVLPS	0 * SIZE(X), %xmm4
	movhps	1 * SIZE(X), %xmm4
	addl	INCX, X
	addpd	%xmm3, %xmm1

	pshufd	$0x4e, %xmm7, %xmm3
	mulpd	%xmm5, %xmm7
	addpd	%xmm7, %xmm0
	MOVLPS	0 * SIZE(Y), %xmm7
	movhps	1 * SIZE(Y), %xmm7
	addl	INCY, Y
	mulpd	%xmm5, %xmm3
	MOVLPS	0 * SIZE(X), %xmm5
	movhps	1 * SIZE(X), %xmm5
	addl	INCX, X
	addpd	%xmm3, %xmm1

	pshufd	$0x4e, %xmm6, %xmm3
	mulpd	%xmm4, %xmm6
	addpd	%xmm6, %xmm0
	MOVLPS	0 * SIZE(Y), %xmm6
	movhps	1 * SIZE(Y), %xmm6
	addl	INCY, Y
	mulpd	%xmm4, %xmm3
	MOVLPS	0 * SIZE(X), %xmm4
	movhps	1 * SIZE(X), %xmm4
	addl	INCX, X
	addpd	%xmm3, %xmm1

	pshufd	$0x4e, %xmm7, %xmm3
	mulpd	%xmm5, %xmm7
	addpd	%xmm7, %xmm0
	MOVLPS	0 * SIZE(Y), %xmm7
	movhps	1 * SIZE(Y), %xmm7
	addl	INCY, Y
	mulpd	%xmm5, %xmm3
	MOVLPS	0 * SIZE(X), %xmm5
	movhps	1 * SIZE(X), %xmm5
	addl	INCX, X
	addpd	%xmm3, %xmm1

	decl	%eax
	jg	.L53
	ALIGN_3

.L54:
	pshufd	$0x4e, %xmm6, %xmm3
	mulpd	%xmm4, %xmm6
	addpd	%xmm6, %xmm0
	MOVLPS	0 * SIZE(Y), %xmm6
	movhps	1 * SIZE(Y), %xmm6
	addl	INCY, Y
	mulpd	%xmm4, %xmm3
	MOVLPS	0 * SIZE(X), %xmm4
	movhps	1 * SIZE(X), %xmm4
	addl	INCX, X
	addpd	%xmm3, %xmm1

	pshufd	$0x4e, %xmm7, %xmm3
	mulpd	%xmm5, %xmm7
	addpd	%xmm7, %xmm0
	MOVLPS	0 * SIZE(Y), %xmm7
	movhps	1 * SIZE(Y), %xmm7
	addl	INCY, Y
	mulpd	%xmm5, %xmm3
	MOVLPS	0 * SIZE(X), %xmm5
	movhps	1 * SIZE(X), %xmm5
	addl	INCX, X
	addpd	%xmm3, %xmm1

	pshufd	$0x4e, %xmm6, %xmm3
	mulpd	%xmm4, %xmm6
	addpd	%xmm6, %xmm0
	MOVLPS	0 * SIZE(Y), %xmm6
	movhps	1 * SIZE(Y), %xmm6
	addl	INCY, Y
	mulpd	%xmm4, %xmm3
	MOVLPS	0 * SIZE(X), %xmm4
	movhps	1 * SIZE(X), %xmm4
	addl	INCX, X
	addpd	%xmm3, %xmm1

	pshufd	$0x4e, %xmm7, %xmm3
	mulpd	%xmm5, %xmm7
	addpd	%xmm7, %xmm0
	MOVLPS	0 * SIZE(Y), %xmm7
	movhps	1 * SIZE(Y), %xmm7
	addl	INCY, Y
	mulpd	%xmm5, %xmm3
	MOVLPS	0 * SIZE(X), %xmm5
	movhps	1 * SIZE(X), %xmm5
	addl	INCX, X
	addpd	%xmm3, %xmm1

	pshufd	$0x4e, %xmm6, %xmm3
	mulpd	%xmm4, %xmm6
	addpd	%xmm6, %xmm0
	MOVLPS	0 * SIZE(Y), %xmm6
	movhps	1 * SIZE(Y), %xmm6
	addl	INCY, Y
	mulpd	%xmm4, %xmm3
	MOVLPS	0 * SIZE(X), %xmm4
	movhps	1 * SIZE(X), %xmm4
	addl	INCX, X
	addpd	%xmm3, %xmm1

	pshufd	$0x4e, %xmm7, %xmm3
	mulpd	%xmm5, %xmm7
	addpd	%xmm7, %xmm0
	MOVLPS	0 * SIZE(Y), %xmm7
	movhps	1 * SIZE(Y), %xmm7
	addl	INCY, Y
	mulpd	%xmm5, %xmm3
	MOVLPS	0 * SIZE(X), %xmm5
	movhps	1 * SIZE(X), %xmm5
	addl	INCX, X
	addpd	%xmm3, %xmm1

	pshufd	$0x4e, %xmm6, %xmm3
	mulpd	%xmm4, %xmm6
	addpd	%xmm6, %xmm0
	mulpd	%xmm4, %xmm3
	addpd	%xmm3, %xmm1

	pshufd	$0x4e, %xmm7, %xmm3
	mulpd	%xmm5, %xmm7
	addpd	%xmm7, %xmm0
	mulpd	%xmm5, %xmm3
	addpd	%xmm3, %xmm1
	ALIGN_3

.L55:
	testl	$4, N
	jle	.L56

	MOVLPS	0 * SIZE(X), %xmm4
	movhps	1 * SIZE(X), %xmm4
	addl	INCX, X
	MOVLPS	0 * SIZE(Y), %xmm6
	movhps	1 * SIZE(Y), %xmm6
	addl	INCY, Y

	MOVLPS	0 * SIZE(X), %xmm5
	movhps	1 * SIZE(X), %xmm5
	addl	INCX, X
	MOVLPS	0 * SIZE(Y), %xmm7
	movhps	1 * SIZE(Y), %xmm7
	addl	INCY, Y

	pshufd	$0x4e, %xmm6, %xmm3
	mulpd	%xmm4, %xmm6
	addpd	%xmm6, %xmm0
	MOVLPS	0 * SIZE(Y), %xmm6
	movhps	1 * SIZE(Y), %xmm6
	addl	INCY, Y
	mulpd	%xmm4, %xmm3
	MOVLPS	0 * SIZE(X), %xmm4
	movhps	1 * SIZE(X), %xmm4
	addl	INCX, X
	addpd	%xmm3, %xmm1

	pshufd	$0x4e, %xmm7, %xmm3
	mulpd	%xmm5, %xmm7
	addpd	%xmm7, %xmm0
	MOVLPS	0 * SIZE(Y), %xmm7
	movhps	1 * SIZE(Y), %xmm7
	addl	INCY, Y
	mulpd	%xmm5, %xmm3
	MOVLPS	0 * SIZE(X), %xmm5
	movhps	1 * SIZE(X), %xmm5
	addl	INCX, X
	addpd	%xmm3, %xmm1

	pshufd	$0x4e, %xmm6, %xmm3
	mulpd	%xmm4, %xmm6
	addpd	%xmm6, %xmm0
	mulpd	%xmm4, %xmm3
	addpd	%xmm3, %xmm1

	pshufd	$0x4e, %xmm7, %xmm3
	mulpd	%xmm5, %xmm7
	addpd	%xmm7, %xmm0
	mulpd	%xmm5, %xmm3
	addpd	%xmm3, %xmm1
	ALIGN_3

.L56:
	testl	$2, N
	jle	.L57

	MOVLPS	0 * SIZE(X), %xmm4
	movhps	1 * SIZE(X), %xmm4
	addl	INCX, X
	MOVLPS	0 * SIZE(Y), %xmm6
	movhps	1 * SIZE(Y), %xmm6
	addl	INCY, Y

	pshufd	$0x4e, %xmm6, %xmm3
	mulpd	%xmm4, %xmm6
	addpd	%xmm6, %xmm0
	mulpd	%xmm4, %xmm3
	addpd	%xmm3, %xmm1

	MOVLPS	0 * SIZE(X), %xmm5
	movhps	1 * SIZE(X), %xmm5
	addl	INCX, X
	MOVLPS	0 * SIZE(Y), %xmm7
	movhps	1 * SIZE(Y), %xmm7
	addl	INCY, Y

	pshufd	$0x4e, %xmm7, %xmm3
	mulpd	%xmm5, %xmm7
	addpd	%xmm7, %xmm0
	mulpd	%xmm5, %xmm3
	addpd	%xmm3, %xmm1
	ALIGN_3

.L57:
	testl	$1, N
	jle	.L98

	MOVLPS	0 * SIZE(X), %xmm4
	movhps	1 * SIZE(X), %xmm4
	MOVLPS	0 * SIZE(Y), %xmm6
	movhps	1 * SIZE(Y), %xmm6

	pshufd	$0x4e, %xmm6, %xmm3
	mulpd	%xmm4, %xmm6
	addpd	%xmm6, %xmm0
	mulpd	%xmm4, %xmm3
	addpd	%xmm3, %xmm1
	ALIGN_3

.L98:
	pshufd	$0x4e, %xmm0, %xmm2
	pshufd	$0x4e, %xmm1, %xmm3

#ifndef CONJ
	subsd	 %xmm2, %xmm0
	addsd	 %xmm3, %xmm1
#else
	addsd	 %xmm2, %xmm0
	subsd	 %xmm3, %xmm1
#endif

.L999:
	movl	RESULT, %eax

	MOVLPS	%xmm0, 0 * SIZE(%eax)
	MOVLPS	%xmm1, 1 * SIZE(%eax)

	popl	%ebx
	popl	%esi
	popl	%edi
#if defined(OS_WINNT) || defined(OS_CYGWIN_NT) || defined(OS_INTERIX)
#ifdef MS_ABI
/* For MingW GCC >= 4.7. It is compatible with MSVC ABI. http://gcc.gnu.org/bugzilla/show_bug.cgi?id=36834 */
	ret
#else
/* remove the hidden return value address from the stack. For MingW GCC < 4.7 */
	ret	$0x4
#endif
#else
/*remove the hidden return value address from the stack on Linux.*/
	ret $0x4
#endif
	EPILOGUE
