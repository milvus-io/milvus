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

#define STACK_N		 4 + STACK + ARGS(%esp)
#define STACK_X		 8 + STACK + ARGS(%esp)
#define STACK_INCX	12 + STACK + ARGS(%esp)
#define STACK_Y		16 + STACK + ARGS(%esp)
#define STACK_INCY	20 + STACK + ARGS(%esp)
#define STACK_C		24 + STACK + ARGS(%esp)
#define STACK_S		28 + STACK + ARGS(%esp)

#define N	%ebx
#define X	%esi
#define INCX	%ecx
#define Y	%edi
#define INCY	%edx

#define	I	%eax

#include "l1param.h"

#define C	%xmm6
#define S	%xmm7

	PROLOGUE

	pushl	%edi
	pushl	%esi
	pushl	%ebx

	PROFCODE

	movl	STACK_N,     N
	movl	STACK_X,     X
	movl	STACK_INCX,  INCX
	movl	STACK_Y,     Y
	movl	STACK_INCY,  INCY

	sall	$ZBASE_SHIFT, INCX
	sall	$ZBASE_SHIFT, INCY

	movss	STACK_C, C
	movss	STACK_S, S

	shufps	$0x0, C, C
	shufps	$0x0, S, S

	cmpl	$0, N
	jle	.L999

	cmpl	$2 * SIZE, INCX
	jne	.L50
	cmpl	$2 * SIZE, INCY
	jne	.L50

	testl	$2 * SIZE, X
	je	.L10

#ifndef HAVE_SSE2
	xorps	%xmm0, %xmm0
	xorps	%xmm1, %xmm1
#endif

	movsd	0 * SIZE(Y), %xmm1
	movsd	0 * SIZE(X), %xmm0

	movaps	%xmm1, %xmm2
	movaps	%xmm0, %xmm3

	mulps	C, %xmm0
	mulps	S, %xmm1

	mulps	C, %xmm2
	mulps	S, %xmm3

	addps	%xmm1, %xmm0
	subps	%xmm3, %xmm2

	movlps	%xmm0, 0 * SIZE(X)
	movlps	%xmm2, 0 * SIZE(Y)
	addl	$2 * SIZE, X
	addl	$2 * SIZE, Y
	decl	N
	jle	.L999

.L10:
	testl	$1 * SIZE, X
	jne	.L30

	testl	$3 * SIZE, Y
	jne	.L20

	movl	N,  I
	sarl	$4, I
	jle	.L14
	ALIGN_3

.L11:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movaps	0 * SIZE(Y), %xmm1
	movaps	0 * SIZE(X), %xmm0

	movaps	%xmm1, %xmm2
	movaps	%xmm0, %xmm3

	mulps	C, %xmm0
	mulps	S, %xmm1

	mulps	C, %xmm2
	mulps	S, %xmm3

	addps	%xmm1, %xmm0
	subps	%xmm3, %xmm2

	movaps	%xmm0, 0 * SIZE(X)
	movaps	%xmm2, 0 * SIZE(Y)

	movaps	4 * SIZE(Y), %xmm1
	movaps	4 * SIZE(X), %xmm0

	movaps	%xmm1, %xmm2
	movaps	%xmm0, %xmm3

	mulps	C, %xmm0
	mulps	S, %xmm1

	mulps	C, %xmm2
	mulps	S, %xmm3

	addps	%xmm1, %xmm0
	subps	%xmm3, %xmm2

	movaps	%xmm0, 4 * SIZE(X)
	movaps	%xmm2, 4 * SIZE(Y)

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	movaps	8 * SIZE(Y), %xmm1
	movaps	8 * SIZE(X), %xmm0

	movaps	%xmm1, %xmm2
	movaps	%xmm0, %xmm3

	mulps	C, %xmm0
	mulps	S, %xmm1

	mulps	C, %xmm2
	mulps	S, %xmm3

	addps	%xmm1, %xmm0
	subps	%xmm3, %xmm2

	movaps	%xmm0, 8 * SIZE(X)
	movaps	%xmm2, 8 * SIZE(Y)

	movaps	12 * SIZE(Y), %xmm1
	movaps	12 * SIZE(X), %xmm0

	movaps	%xmm1, %xmm2
	movaps	%xmm0, %xmm3

	mulps	C, %xmm0
	mulps	S, %xmm1

	mulps	C, %xmm2
	mulps	S, %xmm3

	addps	%xmm1, %xmm0
	subps	%xmm3, %xmm2

	movaps	%xmm0, 12 * SIZE(X)
	movaps	%xmm2, 12 * SIZE(Y)

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE + 64) - PREOFFSET(X)
#endif

	movaps	16 * SIZE(Y), %xmm1
	movaps	16 * SIZE(X), %xmm0

	movaps	%xmm1, %xmm2
	movaps	%xmm0, %xmm3

	mulps	C, %xmm0
	mulps	S, %xmm1

	mulps	C, %xmm2
	mulps	S, %xmm3

	addps	%xmm1, %xmm0
	subps	%xmm3, %xmm2

	movaps	%xmm0, 16 * SIZE(X)
	movaps	%xmm2, 16 * SIZE(Y)

	movaps	20 * SIZE(Y), %xmm1
	movaps	20 * SIZE(X), %xmm0

	movaps	%xmm1, %xmm2
	movaps	%xmm0, %xmm3

	mulps	C, %xmm0
	mulps	S, %xmm1

	mulps	C, %xmm2
	mulps	S, %xmm3

	addps	%xmm1, %xmm0
	subps	%xmm3, %xmm2

	movaps	%xmm0, 20 * SIZE(X)
	movaps	%xmm2, 20 * SIZE(Y)

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE + 64) - PREOFFSET(Y)
#endif

	movaps	24 * SIZE(Y), %xmm1
	movaps	24 * SIZE(X), %xmm0

	movaps	%xmm1, %xmm2
	movaps	%xmm0, %xmm3

	mulps	C, %xmm0
	mulps	S, %xmm1

	mulps	C, %xmm2
	mulps	S, %xmm3

	addps	%xmm1, %xmm0
	subps	%xmm3, %xmm2

	movaps	%xmm0, 24 * SIZE(X)
	movaps	%xmm2, 24 * SIZE(Y)

	movaps	28 * SIZE(Y), %xmm1
	movaps	28 * SIZE(X), %xmm0

	movaps	%xmm1, %xmm2
	movaps	%xmm0, %xmm3

	mulps	C, %xmm0
	mulps	S, %xmm1

	mulps	C, %xmm2
	mulps	S, %xmm3

	addps	%xmm1, %xmm0
	subps	%xmm3, %xmm2

	movaps	%xmm0, 28 * SIZE(X)
	movaps	%xmm2, 28 * SIZE(Y)

	addl	$32 * SIZE, X
	addl	$32 * SIZE, Y

	decl	I
	jg	.L11
	ALIGN_3

.L14:
	testl	$15, N
	jle	.L999

	testl	$8, N
	jle	.L15

	movaps	0 * SIZE(Y), %xmm1
	movaps	0 * SIZE(X), %xmm0

	movaps	%xmm1, %xmm2
	movaps	%xmm0, %xmm3

	mulps	C, %xmm0
	mulps	S, %xmm1

	mulps	C, %xmm2
	mulps	S, %xmm3

	addps	%xmm1, %xmm0
	subps	%xmm3, %xmm2

	movaps	%xmm0, 0 * SIZE(X)
	movaps	%xmm2, 0 * SIZE(Y)

	movaps	4 * SIZE(Y), %xmm1
	movaps	4 * SIZE(X), %xmm0

	movaps	%xmm1, %xmm2
	movaps	%xmm0, %xmm3

	mulps	C, %xmm0
	mulps	S, %xmm1

	mulps	C, %xmm2
	mulps	S, %xmm3

	addps	%xmm1, %xmm0
	subps	%xmm3, %xmm2

	movaps	%xmm0, 4 * SIZE(X)
	movaps	%xmm2, 4 * SIZE(Y)

	movaps	8 * SIZE(Y), %xmm1
	movaps	8 * SIZE(X), %xmm0

	movaps	%xmm1, %xmm2
	movaps	%xmm0, %xmm3

	mulps	C, %xmm0
	mulps	S, %xmm1

	mulps	C, %xmm2
	mulps	S, %xmm3

	addps	%xmm1, %xmm0
	subps	%xmm3, %xmm2

	movaps	%xmm0, 8 * SIZE(X)
	movaps	%xmm2, 8 * SIZE(Y)

	movaps	12 * SIZE(Y), %xmm1
	movaps	12 * SIZE(X), %xmm0

	movaps	%xmm1, %xmm2
	movaps	%xmm0, %xmm3

	mulps	C, %xmm0
	mulps	S, %xmm1

	mulps	C, %xmm2
	mulps	S, %xmm3

	addps	%xmm1, %xmm0
	subps	%xmm3, %xmm2

	movaps	%xmm0, 12 * SIZE(X)
	movaps	%xmm2, 12 * SIZE(Y)

	addl	$16 * SIZE, X
	addl	$16 * SIZE, Y
	ALIGN_3

.L15:
	testl	$4, N
	jle	.L16

	movaps	0 * SIZE(Y), %xmm1
	movaps	0 * SIZE(X), %xmm0

	movaps	%xmm1, %xmm2
	movaps	%xmm0, %xmm3

	mulps	C, %xmm0
	mulps	S, %xmm1

	mulps	C, %xmm2
	mulps	S, %xmm3

	addps	%xmm1, %xmm0
	subps	%xmm3, %xmm2

	movaps	%xmm0, 0 * SIZE(X)
	movaps	%xmm2, 0 * SIZE(Y)

	movaps	4 * SIZE(Y), %xmm1
	movaps	4 * SIZE(X), %xmm0

	movaps	%xmm1, %xmm2
	movaps	%xmm0, %xmm3

	mulps	C, %xmm0
	mulps	S, %xmm1

	mulps	C, %xmm2
	mulps	S, %xmm3

	addps	%xmm1, %xmm0
	subps	%xmm3, %xmm2

	movaps	%xmm0, 4 * SIZE(X)
	movaps	%xmm2, 4 * SIZE(Y)

	addl	$8 * SIZE, X
	addl	$8 * SIZE, Y
	ALIGN_3

.L16:
	testl	$2, N
	jle	.L17

	movaps	0 * SIZE(Y), %xmm1
	movaps	0 * SIZE(X), %xmm0

	movaps	%xmm1, %xmm2
	movaps	%xmm0, %xmm3

	mulps	C, %xmm0
	mulps	S, %xmm1

	mulps	C, %xmm2
	mulps	S, %xmm3

	addps	%xmm1, %xmm0
	subps	%xmm3, %xmm2

	movaps	%xmm0, 0 * SIZE(X)
	movaps	%xmm2, 0 * SIZE(Y)

	addl	$4 * SIZE, X
	addl	$4 * SIZE, Y
	ALIGN_3

.L17:
	testl	$1, N
	jle	.L999

#ifndef HAVE_SSE2
	xorps	%xmm0, %xmm0
	xorps	%xmm1, %xmm1
#endif
	movsd	0 * SIZE(Y), %xmm1
	movsd	0 * SIZE(X), %xmm0

	movaps	%xmm1, %xmm2
	movaps	%xmm0, %xmm3

	mulps	C, %xmm0
	mulps	S, %xmm1

	mulps	C, %xmm2
	mulps	S, %xmm3

	addps	%xmm1, %xmm0
	subps	%xmm3, %xmm2

	movlps	%xmm0, 0 * SIZE(X)
	movlps	%xmm2, 0 * SIZE(Y)
	jmp	.L999
	ALIGN_3

.L20:
	movl	N,  I
	sarl	$4, I
	jle	.L24
	ALIGN_3

.L21:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movsd	0 * SIZE(Y), %xmm1
	movhps	2 * SIZE(Y), %xmm1
	movaps	0 * SIZE(X), %xmm0

	movaps	%xmm1, %xmm2
	movaps	%xmm0, %xmm3

	mulps	C, %xmm0
	mulps	S, %xmm1

	mulps	C, %xmm2
	mulps	S, %xmm3

	addps	%xmm1, %xmm0
	subps	%xmm3, %xmm2

	movaps	%xmm0, 0 * SIZE(X)
	movlps	%xmm2, 0 * SIZE(Y)
	movhps	%xmm2, 2 * SIZE(Y)

	movsd	4 * SIZE(Y), %xmm1
	movhps	6 * SIZE(Y), %xmm1
	movaps	4 * SIZE(X), %xmm0

	movaps	%xmm1, %xmm2
	movaps	%xmm0, %xmm3

	mulps	C, %xmm0
	mulps	S, %xmm1

	mulps	C, %xmm2
	mulps	S, %xmm3

	addps	%xmm1, %xmm0
	subps	%xmm3, %xmm2

	movaps	%xmm0, 4 * SIZE(X)
	movlps	%xmm2, 4 * SIZE(Y)
	movhps	%xmm2, 6 * SIZE(Y)

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	movsd	 8 * SIZE(Y), %xmm1
	movhps	10 * SIZE(Y), %xmm1
	movaps	 8 * SIZE(X), %xmm0

	movaps	%xmm1, %xmm2
	movaps	%xmm0, %xmm3

	mulps	C, %xmm0
	mulps	S, %xmm1

	mulps	C, %xmm2
	mulps	S, %xmm3

	addps	%xmm1, %xmm0
	subps	%xmm3, %xmm2

	movaps	%xmm0,  8 * SIZE(X)
	movlps	%xmm2,  8 * SIZE(Y)
	movhps	%xmm2, 10 * SIZE(Y)

	movsd	12 * SIZE(Y), %xmm1
	movhps	14 * SIZE(Y), %xmm1
	movaps	12 * SIZE(X), %xmm0

	movaps	%xmm1, %xmm2
	movaps	%xmm0, %xmm3

	mulps	C, %xmm0
	mulps	S, %xmm1

	mulps	C, %xmm2
	mulps	S, %xmm3

	addps	%xmm1, %xmm0
	subps	%xmm3, %xmm2

	movaps	%xmm0, 12 * SIZE(X)
	movlps	%xmm2, 12 * SIZE(Y)
	movhps	%xmm2, 14 * SIZE(Y)

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE + 64) - PREOFFSET(X)
#endif

	movsd	16 * SIZE(Y), %xmm1
	movhps	18 * SIZE(Y), %xmm1
	movaps	16 * SIZE(X), %xmm0

	movaps	%xmm1, %xmm2
	movaps	%xmm0, %xmm3

	mulps	C, %xmm0
	mulps	S, %xmm1

	mulps	C, %xmm2
	mulps	S, %xmm3

	addps	%xmm1, %xmm0
	subps	%xmm3, %xmm2

	movaps	%xmm0, 16 * SIZE(X)
	movlps	%xmm2, 16 * SIZE(Y)
	movhps	%xmm2, 18 * SIZE(Y)

	movsd	20 * SIZE(Y), %xmm1
	movhps	22 * SIZE(Y), %xmm1
	movaps	20 * SIZE(X), %xmm0

	movaps	%xmm1, %xmm2
	movaps	%xmm0, %xmm3

	mulps	C, %xmm0
	mulps	S, %xmm1

	mulps	C, %xmm2
	mulps	S, %xmm3

	addps	%xmm1, %xmm0
	subps	%xmm3, %xmm2

	movaps	%xmm0, 20 * SIZE(X)
	movlps	%xmm2, 20 * SIZE(Y)
	movhps	%xmm2, 22 * SIZE(Y)

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE + 64) - PREOFFSET(Y)
#endif

	movsd	24 * SIZE(Y), %xmm1
	movhps	26 * SIZE(Y), %xmm1
	movaps	24 * SIZE(X), %xmm0

	movaps	%xmm1, %xmm2
	movaps	%xmm0, %xmm3

	mulps	C, %xmm0
	mulps	S, %xmm1

	mulps	C, %xmm2
	mulps	S, %xmm3

	addps	%xmm1, %xmm0
	subps	%xmm3, %xmm2

	movaps	%xmm0, 24 * SIZE(X)
	movlps	%xmm2, 24 * SIZE(Y)
	movhps	%xmm2, 26 * SIZE(Y)

	movsd	28 * SIZE(Y), %xmm1
	movhps	30 * SIZE(Y), %xmm1
	movaps	28 * SIZE(X), %xmm0

	movaps	%xmm1, %xmm2
	movaps	%xmm0, %xmm3

	mulps	C, %xmm0
	mulps	S, %xmm1

	mulps	C, %xmm2
	mulps	S, %xmm3

	addps	%xmm1, %xmm0
	subps	%xmm3, %xmm2

	movaps	%xmm0, 28 * SIZE(X)
	movlps	%xmm2, 28 * SIZE(Y)
	movhps	%xmm2, 30 * SIZE(Y)

	addl	$32 * SIZE, X
	addl	$32 * SIZE, Y

	decl	I
	jg	.L21
	ALIGN_3

.L24:
	testl	$15, N
	jle	.L999

	testl	$8, N
	jle	.L25

	movsd	0 * SIZE(Y), %xmm1
	movhps	2 * SIZE(Y), %xmm1
	movaps	0 * SIZE(X), %xmm0

	movaps	%xmm1, %xmm2
	movaps	%xmm0, %xmm3

	mulps	C, %xmm0
	mulps	S, %xmm1

	mulps	C, %xmm2
	mulps	S, %xmm3

	addps	%xmm1, %xmm0
	subps	%xmm3, %xmm2

	movaps	%xmm0, 0 * SIZE(X)
	movlps	%xmm2, 0 * SIZE(Y)
	movhps	%xmm2, 2 * SIZE(Y)

	movsd	4 * SIZE(Y), %xmm1
	movhps	6 * SIZE(Y), %xmm1
	movaps	4 * SIZE(X), %xmm0

	movaps	%xmm1, %xmm2
	movaps	%xmm0, %xmm3

	mulps	C, %xmm0
	mulps	S, %xmm1

	mulps	C, %xmm2
	mulps	S, %xmm3

	addps	%xmm1, %xmm0
	subps	%xmm3, %xmm2

	movaps	%xmm0, 4 * SIZE(X)
	movlps	%xmm2, 4 * SIZE(Y)
	movhps	%xmm2, 6 * SIZE(Y)

	movsd	 8 * SIZE(Y), %xmm1
	movhps	10 * SIZE(Y), %xmm1
	movaps	 8 * SIZE(X), %xmm0

	movaps	%xmm1, %xmm2
	movaps	%xmm0, %xmm3

	mulps	C, %xmm0
	mulps	S, %xmm1

	mulps	C, %xmm2
	mulps	S, %xmm3

	addps	%xmm1, %xmm0
	subps	%xmm3, %xmm2

	movaps	%xmm0,  8 * SIZE(X)
	movlps	%xmm2,  8 * SIZE(Y)
	movhps	%xmm2, 10 * SIZE(Y)

	movsd	12 * SIZE(Y), %xmm1
	movhps	14 * SIZE(Y), %xmm1
	movaps	12 * SIZE(X), %xmm0

	movaps	%xmm1, %xmm2
	movaps	%xmm0, %xmm3

	mulps	C, %xmm0
	mulps	S, %xmm1

	mulps	C, %xmm2
	mulps	S, %xmm3

	addps	%xmm1, %xmm0
	subps	%xmm3, %xmm2

	movaps	%xmm0, 12 * SIZE(X)
	movlps	%xmm2, 12 * SIZE(Y)
	movhps	%xmm2, 14 * SIZE(Y)

	addl	$16 * SIZE, X
	addl	$16 * SIZE, Y
	ALIGN_3

.L25:
	testl	$4, N
	jle	.L26

	movsd	0 * SIZE(Y), %xmm1
	movhps	2 * SIZE(Y), %xmm1
	movaps	0 * SIZE(X), %xmm0

	movaps	%xmm1, %xmm2
	movaps	%xmm0, %xmm3

	mulps	C, %xmm0
	mulps	S, %xmm1

	mulps	C, %xmm2
	mulps	S, %xmm3

	addps	%xmm1, %xmm0
	subps	%xmm3, %xmm2

	movaps	%xmm0, 0 * SIZE(X)
	movlps	%xmm2, 0 * SIZE(Y)
	movhps	%xmm2, 2 * SIZE(Y)

	movsd	4 * SIZE(Y), %xmm1
	movhps	6 * SIZE(Y), %xmm1
	movaps	4 * SIZE(X), %xmm0

	movaps	%xmm1, %xmm2
	movaps	%xmm0, %xmm3

	mulps	C, %xmm0
	mulps	S, %xmm1

	mulps	C, %xmm2
	mulps	S, %xmm3

	addps	%xmm1, %xmm0
	subps	%xmm3, %xmm2

	movaps	%xmm0, 4 * SIZE(X)
	movlps	%xmm2, 4 * SIZE(Y)
	movhps	%xmm2, 6 * SIZE(Y)

	addl	$8 * SIZE, X
	addl	$8 * SIZE, Y
	ALIGN_3

.L26:
	testl	$2, N
	jle	.L27

	movsd	0 * SIZE(Y), %xmm1
	movhps	2 * SIZE(Y), %xmm1
	movaps	0 * SIZE(X), %xmm0

	movaps	%xmm1, %xmm2
	movaps	%xmm0, %xmm3

	mulps	C, %xmm0
	mulps	S, %xmm1

	mulps	C, %xmm2
	mulps	S, %xmm3

	addps	%xmm1, %xmm0
	subps	%xmm3, %xmm2

	movaps	%xmm0, 0 * SIZE(X)
	movlps	%xmm2, 0 * SIZE(Y)
	movhps	%xmm2, 2 * SIZE(Y)

	addl	$4 * SIZE, X
	addl	$4 * SIZE, Y
	ALIGN_3

.L27:
	testl	$1, N
	jle	.L999

	movsd	0 * SIZE(Y), %xmm1
	movsd	0 * SIZE(X), %xmm0

	movaps	%xmm1, %xmm2
	movaps	%xmm0, %xmm3

	mulps	C, %xmm0
	mulps	S, %xmm1

	mulps	C, %xmm2
	mulps	S, %xmm3

	addps	%xmm1, %xmm0
	subps	%xmm3, %xmm2

	movlps	%xmm0, 0 * SIZE(X)
	movlps	%xmm2, 0 * SIZE(Y)
	jmp	.L999
	ALIGN_3

.L30:
	movl	N,  I
	sarl	$4, I
	jle	.L34
	ALIGN_3

.L31:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movsd	0 * SIZE(Y), %xmm1
	movhps	2 * SIZE(Y), %xmm1
	movsd	0 * SIZE(X), %xmm0
	movhps	2 * SIZE(X), %xmm0

	movaps	%xmm1, %xmm2
	movaps	%xmm0, %xmm3

	mulps	C, %xmm0
	mulps	S, %xmm1

	mulps	C, %xmm2
	mulps	S, %xmm3

	addps	%xmm1, %xmm0
	subps	%xmm3, %xmm2

	movlps	%xmm0, 0 * SIZE(X)
	movhps	%xmm0, 2 * SIZE(X)
	movlps	%xmm2, 0 * SIZE(Y)
	movhps	%xmm2, 2 * SIZE(Y)

	movsd	4 * SIZE(Y), %xmm1
	movhps	6 * SIZE(Y), %xmm1
	movsd	4 * SIZE(X), %xmm0
	movhps	6 * SIZE(X), %xmm0

	movaps	%xmm1, %xmm2
	movaps	%xmm0, %xmm3

	mulps	C, %xmm0
	mulps	S, %xmm1

	mulps	C, %xmm2
	mulps	S, %xmm3

	addps	%xmm1, %xmm0
	subps	%xmm3, %xmm2

	movlps	%xmm0, 4 * SIZE(X)
	movhps	%xmm0, 6 * SIZE(X)
	movlps	%xmm2, 4 * SIZE(Y)
	movhps	%xmm2, 6 * SIZE(Y)

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	movsd	 8 * SIZE(Y), %xmm1
	movhps	10 * SIZE(Y), %xmm1
	movsd	 8 * SIZE(X), %xmm0
	movhps	10 * SIZE(X), %xmm0

	movaps	%xmm1, %xmm2
	movaps	%xmm0, %xmm3

	mulps	C, %xmm0
	mulps	S, %xmm1

	mulps	C, %xmm2
	mulps	S, %xmm3

	addps	%xmm1, %xmm0
	subps	%xmm3, %xmm2

	movlps	%xmm0,  8 * SIZE(X)
	movhps	%xmm0, 10 * SIZE(X)
	movlps	%xmm2,  8 * SIZE(Y)
	movhps	%xmm2, 10 * SIZE(Y)

	movsd	12 * SIZE(Y), %xmm1
	movhps	14 * SIZE(Y), %xmm1
	movsd	12 * SIZE(X), %xmm0
	movhps	14 * SIZE(X), %xmm0

	movaps	%xmm1, %xmm2
	movaps	%xmm0, %xmm3

	mulps	C, %xmm0
	mulps	S, %xmm1

	mulps	C, %xmm2
	mulps	S, %xmm3

	addps	%xmm1, %xmm0
	subps	%xmm3, %xmm2

	movlps	%xmm0, 12 * SIZE(X)
	movhps	%xmm0, 14 * SIZE(X)
	movlps	%xmm2, 12 * SIZE(Y)
	movhps	%xmm2, 14 * SIZE(Y)

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE + 64) - PREOFFSET(X)
#endif

	movsd	16 * SIZE(Y), %xmm1
	movhps	18 * SIZE(Y), %xmm1
	movsd	16 * SIZE(X), %xmm0
	movhps	18 * SIZE(X), %xmm0

	movaps	%xmm1, %xmm2
	movaps	%xmm0, %xmm3

	mulps	C, %xmm0
	mulps	S, %xmm1

	mulps	C, %xmm2
	mulps	S, %xmm3

	addps	%xmm1, %xmm0
	subps	%xmm3, %xmm2

	movlps	%xmm0, 16 * SIZE(X)
	movhps	%xmm0, 18 * SIZE(X)
	movlps	%xmm2, 16 * SIZE(Y)
	movhps	%xmm2, 18 * SIZE(Y)

	movsd	20 * SIZE(Y), %xmm1
	movhps	22 * SIZE(Y), %xmm1
	movsd	20 * SIZE(X), %xmm0
	movhps	22 * SIZE(X), %xmm0

	movaps	%xmm1, %xmm2
	movaps	%xmm0, %xmm3

	mulps	C, %xmm0
	mulps	S, %xmm1

	mulps	C, %xmm2
	mulps	S, %xmm3

	addps	%xmm1, %xmm0
	subps	%xmm3, %xmm2

	movlps	%xmm0, 20 * SIZE(X)
	movhps	%xmm0, 22 * SIZE(X)
	movlps	%xmm2, 20 * SIZE(Y)
	movhps	%xmm2, 22 * SIZE(Y)

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE + 64) - PREOFFSET(Y)
#endif

	movsd	24 * SIZE(Y), %xmm1
	movhps	26 * SIZE(Y), %xmm1
	movsd	24 * SIZE(X), %xmm0
	movhps	26 * SIZE(X), %xmm0

	movaps	%xmm1, %xmm2
	movaps	%xmm0, %xmm3

	mulps	C, %xmm0
	mulps	S, %xmm1

	mulps	C, %xmm2
	mulps	S, %xmm3

	addps	%xmm1, %xmm0
	subps	%xmm3, %xmm2

	movlps	%xmm0, 24 * SIZE(X)
	movhps	%xmm0, 26 * SIZE(X)
	movlps	%xmm2, 24 * SIZE(Y)
	movhps	%xmm2, 26 * SIZE(Y)

	movsd	28 * SIZE(Y), %xmm1
	movhps	30 * SIZE(Y), %xmm1
	movsd	28 * SIZE(X), %xmm0
	movhps	30 * SIZE(X), %xmm0

	movaps	%xmm1, %xmm2
	movaps	%xmm0, %xmm3

	mulps	C, %xmm0
	mulps	S, %xmm1

	mulps	C, %xmm2
	mulps	S, %xmm3

	addps	%xmm1, %xmm0
	subps	%xmm3, %xmm2

	movlps	%xmm0, 28 * SIZE(X)
	movhps	%xmm0, 30 * SIZE(X)
	movlps	%xmm2, 28 * SIZE(Y)
	movhps	%xmm2, 30 * SIZE(Y)

	addl	$32 * SIZE, X
	addl	$32 * SIZE, Y

	decl	I
	jg	.L31
	ALIGN_3

.L34:
	testl	$15, N
	jle	.L999

	testl	$8, N
	jle	.L35

	movsd	0 * SIZE(Y), %xmm1
	movhps	2 * SIZE(Y), %xmm1
	movsd	0 * SIZE(X), %xmm0
	movhps	2 * SIZE(X), %xmm0

	movaps	%xmm1, %xmm2
	movaps	%xmm0, %xmm3

	mulps	C, %xmm0
	mulps	S, %xmm1

	mulps	C, %xmm2
	mulps	S, %xmm3

	addps	%xmm1, %xmm0
	subps	%xmm3, %xmm2

	movlps	%xmm0, 0 * SIZE(X)
	movhps	%xmm0, 2 * SIZE(X)
	movlps	%xmm2, 0 * SIZE(Y)
	movhps	%xmm2, 2 * SIZE(Y)

	movsd	4 * SIZE(Y), %xmm1
	movhps	6 * SIZE(Y), %xmm1
	movsd	4 * SIZE(X), %xmm0
	movhps	6 * SIZE(X), %xmm0

	movaps	%xmm1, %xmm2
	movaps	%xmm0, %xmm3

	mulps	C, %xmm0
	mulps	S, %xmm1

	mulps	C, %xmm2
	mulps	S, %xmm3

	addps	%xmm1, %xmm0
	subps	%xmm3, %xmm2

	movlps	%xmm0, 4 * SIZE(X)
	movhps	%xmm0, 6 * SIZE(X)
	movlps	%xmm2, 4 * SIZE(Y)
	movhps	%xmm2, 6 * SIZE(Y)

	movsd	 8 * SIZE(Y), %xmm1
	movhps	10 * SIZE(Y), %xmm1
	movsd	 8 * SIZE(X), %xmm0
	movhps	10 * SIZE(X), %xmm0

	movaps	%xmm1, %xmm2
	movaps	%xmm0, %xmm3

	mulps	C, %xmm0
	mulps	S, %xmm1

	mulps	C, %xmm2
	mulps	S, %xmm3

	addps	%xmm1, %xmm0
	subps	%xmm3, %xmm2

	movlps	%xmm0,  8 * SIZE(X)
	movhps	%xmm0, 10 * SIZE(X)
	movlps	%xmm2,  8 * SIZE(Y)
	movhps	%xmm2, 10 * SIZE(Y)

	movsd	12 * SIZE(Y), %xmm1
	movhps	14 * SIZE(Y), %xmm1
	movsd	12 * SIZE(X), %xmm0
	movhps	14 * SIZE(X), %xmm0

	movaps	%xmm1, %xmm2
	movaps	%xmm0, %xmm3

	mulps	C, %xmm0
	mulps	S, %xmm1

	mulps	C, %xmm2
	mulps	S, %xmm3

	addps	%xmm1, %xmm0
	subps	%xmm3, %xmm2

	movlps	%xmm0, 12 * SIZE(X)
	movhps	%xmm0, 14 * SIZE(X)
	movlps	%xmm2, 12 * SIZE(Y)
	movhps	%xmm2, 14 * SIZE(Y)

	addl	$16 * SIZE, X
	addl	$16 * SIZE, Y
	ALIGN_3

.L35:
	testl	$4, N
	jle	.L36

	movsd	0 * SIZE(Y), %xmm1
	movhps	2 * SIZE(Y), %xmm1
	movsd	0 * SIZE(X), %xmm0
	movhps	2 * SIZE(X), %xmm0

	movaps	%xmm1, %xmm2
	movaps	%xmm0, %xmm3

	mulps	C, %xmm0
	mulps	S, %xmm1

	mulps	C, %xmm2
	mulps	S, %xmm3

	addps	%xmm1, %xmm0
	subps	%xmm3, %xmm2

	movlps	%xmm0, 0 * SIZE(X)
	movhps	%xmm0, 2 * SIZE(X)
	movlps	%xmm2, 0 * SIZE(Y)
	movhps	%xmm2, 2 * SIZE(Y)

	movsd	4 * SIZE(Y), %xmm1
	movhps	6 * SIZE(Y), %xmm1
	movsd	4 * SIZE(X), %xmm0
	movhps	6 * SIZE(X), %xmm0

	movaps	%xmm1, %xmm2
	movaps	%xmm0, %xmm3

	mulps	C, %xmm0
	mulps	S, %xmm1

	mulps	C, %xmm2
	mulps	S, %xmm3

	addps	%xmm1, %xmm0
	subps	%xmm3, %xmm2

	movlps	%xmm0, 4 * SIZE(X)
	movhps	%xmm0, 6 * SIZE(X)
	movlps	%xmm2, 4 * SIZE(Y)
	movhps	%xmm2, 6 * SIZE(Y)

	addl	$8 * SIZE, X
	addl	$8 * SIZE, Y
	ALIGN_3

.L36:
	testl	$2, N
	jle	.L37

	movsd	0 * SIZE(Y), %xmm1
	movhps	2 * SIZE(Y), %xmm1
	movsd	0 * SIZE(X), %xmm0
	movhps	2 * SIZE(X), %xmm0

	movaps	%xmm1, %xmm2
	movaps	%xmm0, %xmm3

	mulps	C, %xmm0
	mulps	S, %xmm1

	mulps	C, %xmm2
	mulps	S, %xmm3

	addps	%xmm1, %xmm0
	subps	%xmm3, %xmm2

	movlps	%xmm0, 0 * SIZE(X)
	movhps	%xmm0, 2 * SIZE(X)
	movlps	%xmm2, 0 * SIZE(Y)
	movhps	%xmm2, 2 * SIZE(Y)

	addl	$4 * SIZE, X
	addl	$4 * SIZE, Y
	ALIGN_3

.L37:
	testl	$1, N
	jle	.L999

#ifndef HAVE_SSE2
	xorps	%xmm0, %xmm0
	xorps	%xmm1, %xmm1
#endif

	movsd	0 * SIZE(Y), %xmm1
	movsd	0 * SIZE(X), %xmm0

	movaps	%xmm1, %xmm2
	movaps	%xmm0, %xmm3

	mulps	C, %xmm0
	mulps	S, %xmm1

	mulps	C, %xmm2
	mulps	S, %xmm3

	addps	%xmm1, %xmm0
	subps	%xmm3, %xmm2

	movlps	%xmm0, 0 * SIZE(X)
	movlps	%xmm2, 0 * SIZE(Y)
	jmp	.L999
	ALIGN_3
	ALIGN_3

.L50:
	movl	N,  I
//if incx ==0 || incy==0 jump to the tail
	cmpl	$0, INCX
	je  .L56
	cmpl	$0, INCY
	je  .L56

	sarl	$2, I
	jle	.L55
	ALIGN_3

.L53:
	movsd	(Y), %xmm1
	movhps	(Y, INCY), %xmm1
	movsd	(X), %xmm0
	movhps	(X, INCX), %xmm0

	movaps	%xmm1, %xmm2
	movaps	%xmm0, %xmm3

	mulps	C, %xmm0
	mulps	S, %xmm1

	mulps	C, %xmm2
	mulps	S, %xmm3

	addps	%xmm1, %xmm0
	subps	%xmm3, %xmm2

	movlps	%xmm0, (X)
	movhps	%xmm0, (X, INCX)
	movlps	%xmm2, (Y)
	movhps	%xmm2, (Y, INCY)

	leal	(X, INCX, 2), X
	leal	(Y, INCY, 2), Y

	movsd	(Y), %xmm1
	movhps	(Y, INCY), %xmm1
	movsd	(X), %xmm0
	movhps	(X, INCX), %xmm0

	movaps	%xmm1, %xmm2
	movaps	%xmm0, %xmm3

	mulps	C, %xmm0
	mulps	S, %xmm1

	mulps	C, %xmm2
	mulps	S, %xmm3

	addps	%xmm1, %xmm0
	subps	%xmm3, %xmm2

	movlps	%xmm0, (X)
	movhps	%xmm0, (X, INCX)
	movlps	%xmm2, (Y)
	movhps	%xmm2, (Y, INCY)

	leal	(X, INCX, 2), X
	leal	(Y, INCY, 2), Y

	decl	I
	jg	.L53
	ALIGN_3

.L55:
#ifndef HAVE_SSE2
	xorps	%xmm0, %xmm0
	xorps	%xmm1, %xmm1
#endif

	movl	N,  I
	andl	$3, I
	jle	.L999
	ALIGN_3

.L56:
	movsd	(Y), %xmm1
	movsd	(X), %xmm0

	movaps	%xmm1, %xmm2
	movaps	%xmm0, %xmm3

	mulps	C, %xmm0
	mulps	S, %xmm1

	mulps	C, %xmm2
	mulps	S, %xmm3

	addps	%xmm1, %xmm0
	subps	%xmm3, %xmm2

	movlps	%xmm0, (X)
	movlps	%xmm2, (Y)

	addl	INCX, X
	addl	INCY, Y

	decl	I
	jg	.L56
	ALIGN_3

.L999:
	popl	%ebx
	popl	%esi
	popl	%edi

	ret

	EPILOGUE
