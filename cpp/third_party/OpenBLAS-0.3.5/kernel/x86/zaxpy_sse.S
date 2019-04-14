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
#define STACK_ALPHA_I	20 + STACK + ARGS(%esp)
#define STACK_X		24 + STACK + ARGS(%esp)
#define STACK_INCX	28 + STACK + ARGS(%esp)
#define STACK_Y		32 + STACK + ARGS(%esp)
#define STACK_INCY	36 + STACK + ARGS(%esp)

#define M	%ebx
#define X	%esi
#define INCX	%ecx
#define Y	%edi
#define INCY	%edx
#define YY	%ebp

#define ALPHA_R	%xmm6
#define ALPHA_I	%xmm7

#include "l1param.h"

	PROLOGUE
	PROFCODE

	pushl	%edi
	pushl	%esi
	pushl	%ebx
	pushl	%ebp

	movl	STACK_M,    M
	movss	STACK_ALPHA_R, ALPHA_R
	movss	STACK_ALPHA_I, ALPHA_I
	movl	STACK_X,    X
	movl	STACK_INCX, INCX
	movl	STACK_Y,    Y
	movl	STACK_INCY, INCY

	sall	$ZBASE_SHIFT, INCX
	sall	$ZBASE_SHIFT, INCY

	testl	M, M
	jle	.L999

	cmpl	$2 * SIZE, INCX
	jne	.L100
	cmpl	$2 * SIZE, INCY
	jne	.L100

#ifdef HAVE_SSE2
	pcmpeqb	%xmm5, %xmm5
	psllq	$63,   %xmm5
#else
	movl	$0x80000000, STACK_M
	movss	STACK_M, %xmm5
	shufps	$0x11, %xmm5, %xmm5
#endif

	shufps	$0, ALPHA_R, ALPHA_R
	shufps	$0, ALPHA_I, ALPHA_I

#ifndef CONJ
	shufps	$0xb1, %xmm5, %xmm5
	xorps	%xmm5, ALPHA_I
#else
	xorps	%xmm5, ALPHA_R
#endif

	subl	$-32 * SIZE, X
	subl	$-32 * SIZE, Y

	testl	$2 * SIZE, Y
	je	.L10

#ifndef HAVE_SSE2
	xorps	%xmm0, %xmm0
#endif
	movsd	-32 * SIZE(X), %xmm0
#ifndef HAVE_SSE2
	xorps	%xmm1, %xmm1
#endif
	movsd	-32 * SIZE(Y), %xmm1

	PSHUFD2($0xb1,   %xmm0, %xmm5)
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm5

	addps	%xmm5,   %xmm0
	addps	%xmm1,   %xmm0

	movlps	%xmm0, -32 * SIZE(Y)

	addl	$2 * SIZE, X
	addl	$2 * SIZE, Y
	decl	M
	jle	.L999
	ALIGN_2

.L10:
	testl	$SIZE, Y
	jne	.L50

	testl	$3 * SIZE, X
	jne	.L20

	movl	M,  %eax
	sarl	$4, %eax
	jle	.L15

	movaps	-32 * SIZE(X), %xmm0
	movaps	-28 * SIZE(X), %xmm1
	movaps	-24 * SIZE(X), %xmm2
	movaps	-20 * SIZE(X), %xmm3

	decl	%eax
	jle	.L12
	ALIGN_3

.L11:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	PSHUFD2($0xb1,   %xmm0, %xmm5)
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm5
	addps	-32 * SIZE(Y), %xmm0
	addps	%xmm5,   %xmm0
	movaps	%xmm0, -32 * SIZE(Y)
	movaps	-16 * SIZE(X), %xmm0

	PSHUFD2($0xb1,   %xmm1, %xmm5)
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm5
	addps	-28 * SIZE(Y), %xmm1
	addps	%xmm5,   %xmm1
	movaps	%xmm1, -28 * SIZE(Y)
	movaps	-12 * SIZE(X), %xmm1

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	PSHUFD2($0xb1,   %xmm2, %xmm5)
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm5
	addps	-24 * SIZE(Y), %xmm2
	addps	%xmm5,   %xmm2
	movaps	%xmm2, -24 * SIZE(Y)
	movaps	 -8 * SIZE(X), %xmm2

	PSHUFD2($0xb1,   %xmm3, %xmm5)
	mulps	ALPHA_R, %xmm3
	mulps	ALPHA_I, %xmm5
	addps	-20 * SIZE(Y), %xmm3
	addps	%xmm5,   %xmm3
	movaps	%xmm3, -20 * SIZE(Y)
	movaps	 -4 * SIZE(X), %xmm3

#if defined(PREFETCHW) && !defined(FETCH128)
	PREFETCHW (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	PSHUFD2($0xb1,   %xmm0, %xmm5)
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm5
	addps	-16 * SIZE(Y), %xmm0
	addps	%xmm5,   %xmm0
	movaps	%xmm0, -16 * SIZE(Y)
	movaps	  0 * SIZE(X), %xmm0

	PSHUFD2($0xb1,   %xmm1, %xmm5)
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm5
	addps	-12 * SIZE(Y), %xmm1
	addps	%xmm5,   %xmm1
	movaps	%xmm1, -12 * SIZE(Y)
	movaps	  4 * SIZE(X), %xmm1

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	PSHUFD2($0xb1,   %xmm2, %xmm5)
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm5
	addps	 -8 * SIZE(Y), %xmm2
	addps	%xmm5,   %xmm2
	movaps	%xmm2,  -8 * SIZE(Y)
	movaps	  8 * SIZE(X), %xmm2

	PSHUFD2($0xb1,   %xmm3, %xmm5)
	mulps	ALPHA_R, %xmm3
	mulps	ALPHA_I, %xmm5
	addps	 -4 * SIZE(Y), %xmm3
	addps	%xmm5,   %xmm3
	movaps	%xmm3,  -4 * SIZE(Y)
	movaps	 12 * SIZE(X), %xmm3

	subl	$-32 * SIZE, X
	subl	$-32 * SIZE, Y
	decl	%eax
	jg	.L11
	ALIGN_3

.L12:
	PSHUFD2($0xb1,   %xmm0, %xmm5)
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm5
	addps	-32 * SIZE(Y), %xmm0
	addps	%xmm5,   %xmm0
	movaps	%xmm0, -32 * SIZE(Y)
	movaps	-16 * SIZE(X), %xmm0

	PSHUFD2($0xb1,   %xmm1, %xmm5)
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm5
	addps	-28 * SIZE(Y), %xmm1
	addps	%xmm5,   %xmm1
	movaps	%xmm1, -28 * SIZE(Y)
	movaps	-12 * SIZE(X), %xmm1

	PSHUFD2($0xb1,   %xmm2, %xmm5)
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm5
	addps	-24 * SIZE(Y), %xmm2
	addps	%xmm5,   %xmm2
	movaps	%xmm2, -24 * SIZE(Y)
	movaps	 -8 * SIZE(X), %xmm2

	PSHUFD2($0xb1,   %xmm3, %xmm5)
	mulps	ALPHA_R, %xmm3
	mulps	ALPHA_I, %xmm5
	addps	-20 * SIZE(Y), %xmm3
	addps	%xmm5,   %xmm3
	movaps	%xmm3, -20 * SIZE(Y)
	movaps	 -4 * SIZE(X), %xmm3

	PSHUFD2($0xb1,   %xmm0, %xmm5)
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm5
	addps	-16 * SIZE(Y), %xmm0
	addps	%xmm5,   %xmm0
	movaps	%xmm0, -16 * SIZE(Y)

	PSHUFD2($0xb1,   %xmm1, %xmm5)
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm5
	addps	-12 * SIZE(Y), %xmm1
	addps	%xmm5,   %xmm1
	movaps	%xmm1, -12 * SIZE(Y)

	PSHUFD2($0xb1,   %xmm2, %xmm5)
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm5
	addps	 -8 * SIZE(Y), %xmm2
	addps	%xmm5,   %xmm2
	movaps	%xmm2,  -8 * SIZE(Y)

	PSHUFD2($0xb1,   %xmm3, %xmm5)
	mulps	ALPHA_R, %xmm3
	mulps	ALPHA_I, %xmm5
	addps	 -4 * SIZE(Y), %xmm3
	addps	%xmm5,   %xmm3
	movaps	%xmm3,  -4 * SIZE(Y)

	subl	$-32 * SIZE, X
	subl	$-32 * SIZE, Y
	ALIGN_3

.L15:
	testl	$8, M
	jle	.L16

	movaps	-32 * SIZE(X), %xmm0
	movaps	-28 * SIZE(X), %xmm1

	PSHUFD2($0xb1,   %xmm0, %xmm5)
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm5
	addps	-32 * SIZE(Y), %xmm0
	addps	%xmm5,   %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	PSHUFD2($0xb1,   %xmm1, %xmm5)
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm5
	addps	-28 * SIZE(Y), %xmm1
	addps	%xmm5,   %xmm1
	movaps	%xmm1, -28 * SIZE(Y)

	movaps	-24 * SIZE(X), %xmm2
	movaps	-20 * SIZE(X), %xmm3

	PSHUFD2($0xb1,   %xmm2, %xmm5)
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm5
	addps	-24 * SIZE(Y), %xmm2
	addps	%xmm5,   %xmm2
	movaps	%xmm2, -24 * SIZE(Y)

	PSHUFD2($0xb1,   %xmm3, %xmm5)
	mulps	ALPHA_R, %xmm3
	mulps	ALPHA_I, %xmm5
	addps	-20 * SIZE(Y), %xmm3
	addps	%xmm5,   %xmm3
	movaps	%xmm3, -20 * SIZE(Y)

	addl	$16 * SIZE, X
	addl	$16 * SIZE, Y
	ALIGN_2

.L16:
	testl	$4, M
	jle	.L17

	movaps	-32 * SIZE(X), %xmm0
	movaps	-28 * SIZE(X), %xmm1

	PSHUFD2($0xb1,   %xmm0, %xmm5)
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm5
	addps	-32 * SIZE(Y), %xmm0
	addps	%xmm5,   %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	PSHUFD2($0xb1,   %xmm1, %xmm5)
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm5
	addps	-28 * SIZE(Y), %xmm1
	addps	%xmm5,   %xmm1
	movaps	%xmm1, -28 * SIZE(Y)

	addl	$8 * SIZE, X
	addl	$8 * SIZE, Y
	ALIGN_2

.L17:
	testl	$2, M
	jle	.L18

	movaps	-32 * SIZE(X), %xmm0

	PSHUFD2($0xb1,   %xmm0, %xmm5)
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm5

	addps	-32 * SIZE(Y), %xmm0
	addps	%xmm5,   %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	addl	$4 * SIZE, X
	addl	$4 * SIZE, Y
	ALIGN_2

.L18:
	testl	$1, M
	jle	.L999

#ifndef HAVE_SSE2
	xorps	%xmm0, %xmm0
#endif
	movsd	-32 * SIZE(X), %xmm0

	PSHUFD2($0xb1,   %xmm0, %xmm5)
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm5

#ifndef HAVE_SSE2
	xorps	%xmm1, %xmm1
	movlps	-32 * SIZE(Y), %xmm1
#else
	movsd	-32 * SIZE(Y), %xmm1
#endif
	addps	%xmm1, %xmm0
	addps	%xmm5, %xmm0
	movlps	%xmm0, -32 * SIZE(Y)
	jmp	.L999
	ALIGN_3

.L20:
#ifdef ALIGNED_ACCESS

	testl	$2 * SIZE, X
	jne	.L30

	subl	$1 * SIZE, X

	movaps	-32 * SIZE(X), %xmm0

	movl	M,  %eax
	sarl	$4, %eax
	jle	.L25

	movaps	-28 * SIZE(X), %xmm1
	movaps	-24 * SIZE(X), %xmm2
	movaps	-20 * SIZE(X), %xmm3

	decl	%eax
	jle	.L22
	ALIGN_3

.L21:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	movss	 %xmm1,  %xmm0
	SHUFPS_39  %xmm0, %xmm0
	PSHUFD2($0xb1,   %xmm0, %xmm5)
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm5
	addps	-32 * SIZE(Y), %xmm0
	addps	%xmm5,   %xmm0
	movaps	%xmm0, -32 * SIZE(Y)
	movaps	-16 * SIZE(X), %xmm0

	movss	 %xmm2,  %xmm1
	SHUFPS_39  %xmm1, %xmm1
	PSHUFD2($0xb1,   %xmm1, %xmm5)
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm5
	addps	-28 * SIZE(Y), %xmm1
	addps	%xmm5,   %xmm1
	movaps	%xmm1, -28 * SIZE(Y)
	movaps	-12 * SIZE(X), %xmm1

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movss	 %xmm3,  %xmm2
	SHUFPS_39  %xmm2, %xmm2
	PSHUFD2($0xb1,   %xmm2, %xmm5)
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm5
	addps	-24 * SIZE(Y), %xmm2
	addps	%xmm5,   %xmm2
	movaps	%xmm2, -24 * SIZE(Y)
	movaps	 -8 * SIZE(X), %xmm2

	movss	 %xmm0,  %xmm3
	SHUFPS_39  %xmm3, %xmm3
	PSHUFD2($0xb1,   %xmm3, %xmm5)
	mulps	ALPHA_R, %xmm3
	mulps	ALPHA_I, %xmm5
	addps	-20 * SIZE(Y), %xmm3
	addps	%xmm5,   %xmm3
	movaps	%xmm3, -20 * SIZE(Y)
	movaps	 -4 * SIZE(X), %xmm3

#if defined(PREFETCHW) && !defined(FETCH128)
	PREFETCHW (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	movss	 %xmm1,  %xmm0
	SHUFPS_39  %xmm0, %xmm0
	PSHUFD2($0xb1,   %xmm0, %xmm5)
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm5
	addps	-16 * SIZE(Y), %xmm0
	addps	%xmm5,   %xmm0
	movaps	%xmm0, -16 * SIZE(Y)
	movaps	  0 * SIZE(X), %xmm0

	movss	 %xmm2,  %xmm1
	SHUFPS_39  %xmm1, %xmm1
	PSHUFD2($0xb1,   %xmm1, %xmm5)
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm5
	addps	-12 * SIZE(Y), %xmm1
	addps	%xmm5,   %xmm1
	movaps	%xmm1, -12 * SIZE(Y)
	movaps	  4 * SIZE(X), %xmm1

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	movss	 %xmm3,  %xmm2
	SHUFPS_39  %xmm2, %xmm2
	PSHUFD2($0xb1,   %xmm2, %xmm5)
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm5
	addps	 -8 * SIZE(Y), %xmm2
	addps	%xmm5,   %xmm2
	movaps	%xmm2,  -8 * SIZE(Y)
	movaps	  8 * SIZE(X), %xmm2

	movss	 %xmm0,  %xmm3
	SHUFPS_39  %xmm3, %xmm3
	PSHUFD2($0xb1,   %xmm3, %xmm5)
	mulps	ALPHA_R, %xmm3
	mulps	ALPHA_I, %xmm5
	addps	 -4 * SIZE(Y), %xmm3
	addps	%xmm5,   %xmm3
	movaps	%xmm3,  -4 * SIZE(Y)
	movaps	 12 * SIZE(X), %xmm3

	subl	$-32 * SIZE, X
	subl	$-32 * SIZE, Y
	decl	%eax
	jg	.L21
	ALIGN_3

.L22:
	movss	 %xmm1,  %xmm0
	SHUFPS_39  %xmm0, %xmm0
	PSHUFD2($0xb1,   %xmm0, %xmm5)
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm5
	addps	-32 * SIZE(Y), %xmm0
	addps	%xmm5,   %xmm0
	movaps	%xmm0, -32 * SIZE(Y)
	movaps	-16 * SIZE(X), %xmm0

	movss	 %xmm2,  %xmm1
	SHUFPS_39  %xmm1, %xmm1
	PSHUFD2($0xb1,   %xmm1, %xmm5)
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm5
	addps	-28 * SIZE(Y), %xmm1
	addps	%xmm5,   %xmm1
	movaps	%xmm1, -28 * SIZE(Y)
	movaps	-12 * SIZE(X), %xmm1

	movss	 %xmm3,  %xmm2
	SHUFPS_39  %xmm2, %xmm2
	PSHUFD2($0xb1,   %xmm2, %xmm5)
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm5
	addps	-24 * SIZE(Y), %xmm2
	addps	%xmm5,   %xmm2
	movaps	%xmm2, -24 * SIZE(Y)
	movaps	 -8 * SIZE(X), %xmm2

	movss	 %xmm0,  %xmm3
	SHUFPS_39  %xmm3, %xmm3
	PSHUFD2($0xb1,   %xmm3, %xmm5)
	mulps	ALPHA_R, %xmm3
	mulps	ALPHA_I, %xmm5
	addps	-20 * SIZE(Y), %xmm3
	addps	%xmm5,   %xmm3
	movaps	%xmm3, -20 * SIZE(Y)
	movaps	 -4 * SIZE(X), %xmm3

	movss	 %xmm1,  %xmm0
	SHUFPS_39  %xmm0, %xmm0
	PSHUFD2($0xb1,   %xmm0, %xmm5)
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm5
	addps	-16 * SIZE(Y), %xmm0
	addps	%xmm5,   %xmm0
	movaps	%xmm0, -16 * SIZE(Y)
	movaps	  0 * SIZE(X), %xmm0

	movss	 %xmm2,  %xmm1
	SHUFPS_39  %xmm1, %xmm1
	PSHUFD2($0xb1,   %xmm1, %xmm5)
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm5
	addps	-12 * SIZE(Y), %xmm1
	addps	%xmm5,   %xmm1
	movaps	%xmm1, -12 * SIZE(Y)

	movss	 %xmm3,  %xmm2
	SHUFPS_39  %xmm2, %xmm2
	PSHUFD2($0xb1,   %xmm2, %xmm5)
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm5
	addps	 -8 * SIZE(Y), %xmm2
	addps	%xmm5,   %xmm2
	movaps	%xmm2,  -8 * SIZE(Y)

	movss	 %xmm0,  %xmm3
	SHUFPS_39  %xmm3, %xmm3
	PSHUFD2($0xb1,   %xmm3, %xmm5)
	mulps	ALPHA_R, %xmm3
	mulps	ALPHA_I, %xmm5
	addps	 -4 * SIZE(Y), %xmm3
	addps	%xmm5,   %xmm3
	movaps	%xmm3,  -4 * SIZE(Y)

	subl	$-32 * SIZE, X
	subl	$-32 * SIZE, Y
	ALIGN_3

.L25:
	testl	$8, M
	jle	.L26

	movaps	-28 * SIZE(X), %xmm1
	movaps	-24 * SIZE(X), %xmm2

	movss	 %xmm1,  %xmm0
	SHUFPS_39  %xmm0, %xmm0
	PSHUFD2($0xb1,   %xmm0, %xmm5)
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm5
	addps	-32 * SIZE(Y), %xmm0
	addps	%xmm5,   %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	movss	 %xmm2,  %xmm1
	SHUFPS_39  %xmm1, %xmm1
	PSHUFD2($0xb1,   %xmm1, %xmm5)
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm5
	addps	-28 * SIZE(Y), %xmm1
	addps	%xmm5,   %xmm1
	movaps	%xmm1, -28 * SIZE(Y)

	movaps	-20 * SIZE(X), %xmm3
	movaps	-16 * SIZE(X), %xmm0

	movss	 %xmm3,  %xmm2
	SHUFPS_39  %xmm2, %xmm2
	PSHUFD2($0xb1,   %xmm2, %xmm5)
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm5
	addps	-24 * SIZE(Y), %xmm2
	addps	%xmm5,   %xmm2
	movaps	%xmm2, -24 * SIZE(Y)

	movss	 %xmm0,  %xmm3
	SHUFPS_39  %xmm3, %xmm3
	PSHUFD2($0xb1,   %xmm3, %xmm5)
	mulps	ALPHA_R, %xmm3
	mulps	ALPHA_I, %xmm5
	addps	-20 * SIZE(Y), %xmm3
	addps	%xmm5,   %xmm3
	movaps	%xmm3, -20 * SIZE(Y)

	addl	$16 * SIZE, X
	addl	$16 * SIZE, Y
	ALIGN_2

.L26:
	testl	$4, M
	jle	.L27

	movaps	-28 * SIZE(X), %xmm1
	movaps	-24 * SIZE(X), %xmm2

	movss	 %xmm1,  %xmm0
	SHUFPS_39  %xmm0, %xmm0
	PSHUFD2($0xb1,   %xmm0, %xmm5)
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm5
	addps	-32 * SIZE(Y), %xmm0
	addps	%xmm5,   %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	movss	 %xmm2,  %xmm1
	SHUFPS_39  %xmm1, %xmm1
	PSHUFD2($0xb1,   %xmm1, %xmm5)
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm5
	addps	-28 * SIZE(Y), %xmm1
	addps	%xmm5,   %xmm1
	movaps	%xmm1, -28 * SIZE(Y)

	movaps	%xmm2, %xmm0

	addl	$8 * SIZE, X
	addl	$8 * SIZE, Y
	ALIGN_2

.L27:
	testl	$2, M
	jle	.L28

	movaps	-28 * SIZE(X), %xmm1

	movss	 %xmm1,  %xmm0
	SHUFPS_39  %xmm0, %xmm0
	PSHUFD2($0xb1,   %xmm0, %xmm5)
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm5

	addps	-32 * SIZE(Y), %xmm0
	addps	%xmm5,   %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	movaps	%xmm1, %xmm0

	addl	$4 * SIZE, X
	addl	$4 * SIZE, Y
	ALIGN_2

.L28:
	testl	$1, M
	jle	.L999

	PSHUFD2($0x06, %xmm0, %xmm5)
	PSHUFD2($0x09, %xmm0, %xmm0)

	mulps	ALPHA_I, %xmm5
	mulps	ALPHA_R, %xmm0

#ifndef HAVE_SSE2
	xorps	%xmm1, %xmm1
	movlps	-32 * SIZE(Y), %xmm1
#else
	movsd	-32 * SIZE(Y), %xmm1
#endif
	addps	%xmm1, %xmm0
	addps	%xmm5, %xmm0

	movlps	%xmm0, -32 * SIZE(Y)

	jmp	.L999
	ALIGN_3

.L30:
	testl	$1 * SIZE, X
	jne	.L40
#endif

	movl	M,  %eax
	sarl	$4, %eax
	jle	.L35

	movsd	-32 * SIZE(X), %xmm0
	movhps	-30 * SIZE(X), %xmm0
	movsd	-28 * SIZE(X), %xmm1
	movhps	-26 * SIZE(X), %xmm1
	movsd	-24 * SIZE(X), %xmm2
	movhps	-22 * SIZE(X), %xmm2
	movsd	-20 * SIZE(X), %xmm3
	movhps	-18 * SIZE(X), %xmm3

	decl	%eax
	jle	.L32
	ALIGN_3

.L31:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	PSHUFD2($0xb1,   %xmm0, %xmm5)
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm5
	addps	-32 * SIZE(Y), %xmm0
	addps	%xmm5,   %xmm0
	movaps	%xmm0, -32 * SIZE(Y)
	movsd	-16 * SIZE(X), %xmm0
	movhps	-14 * SIZE(X), %xmm0

	PSHUFD2($0xb1,   %xmm1, %xmm5)
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm5
	addps	-28 * SIZE(Y), %xmm1
	addps	%xmm5,   %xmm1
	movaps	%xmm1, -28 * SIZE(Y)
	movsd	-12 * SIZE(X), %xmm1
	movhps	-10 * SIZE(X), %xmm1

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	PSHUFD2($0xb1,   %xmm2, %xmm5)
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm5
	addps	-24 * SIZE(Y), %xmm2
	addps	%xmm5,   %xmm2
	movaps	%xmm2, -24 * SIZE(Y)
	movsd	 -8 * SIZE(X), %xmm2
	movhps	 -6 * SIZE(X), %xmm2

	PSHUFD2($0xb1,   %xmm3, %xmm5)
	mulps	ALPHA_R, %xmm3
	mulps	ALPHA_I, %xmm5
	addps	-20 * SIZE(Y), %xmm3
	addps	%xmm5,   %xmm3
	movaps	%xmm3, -20 * SIZE(Y)
	movsd	 -4 * SIZE(X), %xmm3
	movhps	 -2 * SIZE(X), %xmm3

#if defined(PREFETCHW) && !defined(FETCH128)
	PREFETCHW (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	PSHUFD2($0xb1,   %xmm0, %xmm5)
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm5
	addps	-16 * SIZE(Y), %xmm0
	addps	%xmm5,   %xmm0
	movaps	%xmm0, -16 * SIZE(Y)
	movsd	  0 * SIZE(X), %xmm0
	movhps	  2 * SIZE(X), %xmm0

	PSHUFD2($0xb1,   %xmm1, %xmm5)
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm5
	addps	-12 * SIZE(Y), %xmm1
	addps	%xmm5,   %xmm1
	movaps	%xmm1, -12 * SIZE(Y)
	movsd	  4 * SIZE(X), %xmm1
	movhps	  6 * SIZE(X), %xmm1

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	PSHUFD2($0xb1,   %xmm2, %xmm5)
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm5
	addps	 -8 * SIZE(Y), %xmm2
	addps	%xmm5,   %xmm2
	movaps	%xmm2,  -8 * SIZE(Y)
	movsd	  8 * SIZE(X), %xmm2
	movhps	 10 * SIZE(X), %xmm2

	PSHUFD2($0xb1,   %xmm3, %xmm5)
	mulps	ALPHA_R, %xmm3
	mulps	ALPHA_I, %xmm5
	addps	 -4 * SIZE(Y), %xmm3
	addps	%xmm5,   %xmm3
	movaps	%xmm3,  -4 * SIZE(Y)
	movsd	 12 * SIZE(X), %xmm3
	movhps	 14 * SIZE(X), %xmm3

	subl	$-32 * SIZE, X
	subl	$-32 * SIZE, Y
	decl	%eax
	jg	.L31
	ALIGN_3

.L32:
	PSHUFD2($0xb1,   %xmm0, %xmm5)
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm5
	addps	-32 * SIZE(Y), %xmm0
	addps	%xmm5,   %xmm0
	movaps	%xmm0, -32 * SIZE(Y)
	movsd	-16 * SIZE(X), %xmm0
	movhps	-14 * SIZE(X), %xmm0

	PSHUFD2($0xb1,   %xmm1, %xmm5)
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm5
	addps	-28 * SIZE(Y), %xmm1
	addps	%xmm5,   %xmm1
	movaps	%xmm1, -28 * SIZE(Y)
	movsd	-12 * SIZE(X), %xmm1
	movhps	-10 * SIZE(X), %xmm1

	PSHUFD2($0xb1,   %xmm2, %xmm5)
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm5
	addps	-24 * SIZE(Y), %xmm2
	addps	%xmm5,   %xmm2
	movaps	%xmm2, -24 * SIZE(Y)
	movsd	 -8 * SIZE(X), %xmm2
	movhps	 -6 * SIZE(X), %xmm2

	PSHUFD2($0xb1,   %xmm3, %xmm5)
	mulps	ALPHA_R, %xmm3
	mulps	ALPHA_I, %xmm5
	addps	-20 * SIZE(Y), %xmm3
	addps	%xmm5,   %xmm3
	movaps	%xmm3, -20 * SIZE(Y)
	movsd	 -4 * SIZE(X), %xmm3
	movhps	 -2 * SIZE(X), %xmm3

	PSHUFD2($0xb1,   %xmm0, %xmm5)
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm5
	addps	-16 * SIZE(Y), %xmm0
	addps	%xmm5,   %xmm0
	movaps	%xmm0, -16 * SIZE(Y)

	PSHUFD2($0xb1,   %xmm1, %xmm5)
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm5
	addps	-12 * SIZE(Y), %xmm1
	addps	%xmm5,   %xmm1
	movaps	%xmm1, -12 * SIZE(Y)

	PSHUFD2($0xb1,   %xmm2, %xmm5)
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm5
	addps	 -8 * SIZE(Y), %xmm2
	addps	%xmm5,   %xmm2
	movaps	%xmm2,  -8 * SIZE(Y)

	PSHUFD2($0xb1,   %xmm3, %xmm5)
	mulps	ALPHA_R, %xmm3
	mulps	ALPHA_I, %xmm5
	addps	 -4 * SIZE(Y), %xmm3
	addps	%xmm5,   %xmm3
	movaps	%xmm3,  -4 * SIZE(Y)

	subl	$-32 * SIZE, X
	subl	$-32 * SIZE, Y
	ALIGN_3

.L35:
	testl	$8, M
	jle	.L36

	movsd	-32 * SIZE(X), %xmm0
	movhps	-30 * SIZE(X), %xmm0
	movsd	-28 * SIZE(X), %xmm1
	movhps	-26 * SIZE(X), %xmm1

	PSHUFD2($0xb1,   %xmm0, %xmm5)
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm5
	addps	-32 * SIZE(Y), %xmm0
	addps	%xmm5,   %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	PSHUFD2($0xb1,   %xmm1, %xmm5)
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm5
	addps	-28 * SIZE(Y), %xmm1
	addps	%xmm5,   %xmm1
	movaps	%xmm1, -28 * SIZE(Y)

	movsd	-24 * SIZE(X), %xmm2
	movhps	-22 * SIZE(X), %xmm2
	movsd	-20 * SIZE(X), %xmm3
	movhps	-18 * SIZE(X), %xmm3

	PSHUFD2($0xb1,   %xmm2, %xmm5)
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm5
	addps	-24 * SIZE(Y), %xmm2
	addps	%xmm5,   %xmm2
	movaps	%xmm2, -24 * SIZE(Y)

	PSHUFD2($0xb1,   %xmm3, %xmm5)
	mulps	ALPHA_R, %xmm3
	mulps	ALPHA_I, %xmm5
	addps	-20 * SIZE(Y), %xmm3
	addps	%xmm5,   %xmm3
	movaps	%xmm3, -20 * SIZE(Y)

	addl	$16 * SIZE, X
	addl	$16 * SIZE, Y
	ALIGN_2

.L36:
	testl	$4, M
	jle	.L37

	movsd	-32 * SIZE(X), %xmm0
	movhps	-30 * SIZE(X), %xmm0
	movsd	-28 * SIZE(X), %xmm1
	movhps	-26 * SIZE(X), %xmm1

	PSHUFD2($0xb1,   %xmm0, %xmm5)
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm5
	addps	-32 * SIZE(Y), %xmm0
	addps	%xmm5,   %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	PSHUFD2($0xb1,   %xmm1, %xmm5)
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm5
	addps	-28 * SIZE(Y), %xmm1
	addps	%xmm5,   %xmm1
	movaps	%xmm1, -28 * SIZE(Y)

	addl	$8 * SIZE, X
	addl	$8 * SIZE, Y
	ALIGN_2

.L37:
	testl	$2, M
	jle	.L38

	movsd	-32 * SIZE(X), %xmm0
	movhps	-30 * SIZE(X), %xmm0

	PSHUFD2($0xb1,   %xmm0, %xmm5)
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm5

	addps	-32 * SIZE(Y), %xmm0
	addps	%xmm5,   %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	addl	$4 * SIZE, X
	addl	$4 * SIZE, Y
	ALIGN_2

.L38:
	testl	$1, M
	jle	.L999

	movsd	-32 * SIZE(X), %xmm0

	PSHUFD2($0xb1,   %xmm0, %xmm5)
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm5

	addps	-32 * SIZE(Y), %xmm0
	addps	%xmm5,   %xmm0
	movlps	%xmm0, -32 * SIZE(Y)
	jmp	.L999
	ALIGN_3

#ifdef ALIGNED_ACCESS

.L40:
	subl	$3 * SIZE, X

	movaps	-32 * SIZE(X), %xmm0

	movl	M,  %eax
	sarl	$4, %eax
	jle	.L45

	movaps	-28 * SIZE(X), %xmm1
	movaps	-24 * SIZE(X), %xmm2
	movaps	-20 * SIZE(X), %xmm3

	decl	%eax
	jle	.L42
	ALIGN_3

.L41:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	movss	 %xmm1,  %xmm0
	shufps	 $0x93,  %xmm1, %xmm0
	PSHUFD2($0xb1,   %xmm0, %xmm5)
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm5
	addps	-32 * SIZE(Y), %xmm0
	addps	%xmm5,   %xmm0
	movaps	%xmm0, -32 * SIZE(Y)
	movaps	-16 * SIZE(X), %xmm0

	movss	 %xmm2,  %xmm1
	shufps	 $0x93,  %xmm2, %xmm1
	PSHUFD2($0xb1,   %xmm1, %xmm5)
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm5
	addps	-28 * SIZE(Y), %xmm1
	addps	%xmm5,   %xmm1
	movaps	%xmm1, -28 * SIZE(Y)
	movaps	-12 * SIZE(X), %xmm1

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movss	 %xmm3,  %xmm2
	shufps	 $0x93,  %xmm3, %xmm2
	PSHUFD2($0xb1,   %xmm2, %xmm5)
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm5
	addps	-24 * SIZE(Y), %xmm2
	addps	%xmm5,   %xmm2
	movaps	%xmm2, -24 * SIZE(Y)
	movaps	 -8 * SIZE(X), %xmm2

	movss	 %xmm0,  %xmm3
	shufps	 $0x93,  %xmm0, %xmm3
	PSHUFD2($0xb1,   %xmm3, %xmm5)
	mulps	ALPHA_R, %xmm3
	mulps	ALPHA_I, %xmm5
	addps	-20 * SIZE(Y), %xmm3
	addps	%xmm5,   %xmm3
	movaps	%xmm3, -20 * SIZE(Y)
	movaps	 -4 * SIZE(X), %xmm3

#if defined(PREFETCHW) && !defined(FETCH128)
	PREFETCHW (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	movss	 %xmm1,  %xmm0
	shufps	 $0x93,  %xmm1, %xmm0
	PSHUFD2($0xb1,   %xmm0, %xmm5)
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm5
	addps	-16 * SIZE(Y), %xmm0
	addps	%xmm5,   %xmm0
	movaps	%xmm0, -16 * SIZE(Y)
	movaps	  0 * SIZE(X), %xmm0

	movss	 %xmm2,  %xmm1
	shufps	 $0x93,  %xmm2, %xmm1
	PSHUFD2($0xb1,   %xmm1, %xmm5)
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm5
	addps	-12 * SIZE(Y), %xmm1
	addps	%xmm5,   %xmm1
	movaps	%xmm1, -12 * SIZE(Y)
	movaps	  4 * SIZE(X), %xmm1

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	movss	 %xmm3,  %xmm2
	shufps	 $0x93,  %xmm3, %xmm2
	PSHUFD2($0xb1,   %xmm2, %xmm5)
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm5
	addps	 -8 * SIZE(Y), %xmm2
	addps	%xmm5,   %xmm2
	movaps	%xmm2,  -8 * SIZE(Y)
	movaps	  8 * SIZE(X), %xmm2

	movss	 %xmm0,  %xmm3
	shufps	 $0x93,  %xmm0, %xmm3
	PSHUFD2($0xb1,   %xmm3, %xmm5)
	mulps	ALPHA_R, %xmm3
	mulps	ALPHA_I, %xmm5
	addps	 -4 * SIZE(Y), %xmm3
	addps	%xmm5,   %xmm3
	movaps	%xmm3,  -4 * SIZE(Y)
	movaps	 12 * SIZE(X), %xmm3

	subl	$-32 * SIZE, X
	subl	$-32 * SIZE, Y
	decl	%eax
	jg	.L41
	ALIGN_3

.L42:
	movss	 %xmm1,  %xmm0
	shufps	 $0x93,  %xmm1, %xmm0
	PSHUFD2($0xb1,   %xmm0, %xmm5)
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm5
	addps	-32 * SIZE(Y), %xmm0
	addps	%xmm5,   %xmm0
	movaps	%xmm0, -32 * SIZE(Y)
	movaps	-16 * SIZE(X), %xmm0

	movss	 %xmm2,  %xmm1
	shufps	 $0x93,  %xmm2, %xmm1
	PSHUFD2($0xb1,   %xmm1, %xmm5)
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm5
	addps	-28 * SIZE(Y), %xmm1
	addps	%xmm5,   %xmm1
	movaps	%xmm1, -28 * SIZE(Y)
	movaps	-12 * SIZE(X), %xmm1

	movss	 %xmm3,  %xmm2
	shufps	 $0x93,  %xmm3, %xmm2
	PSHUFD2($0xb1,   %xmm2, %xmm5)
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm5
	addps	-24 * SIZE(Y), %xmm2
	addps	%xmm5,   %xmm2
	movaps	%xmm2, -24 * SIZE(Y)
	movaps	 -8 * SIZE(X), %xmm2

	movss	 %xmm0,  %xmm3
	shufps	 $0x93,  %xmm0, %xmm3
	PSHUFD2($0xb1,   %xmm3, %xmm5)
	mulps	ALPHA_R, %xmm3
	mulps	ALPHA_I, %xmm5
	addps	-20 * SIZE(Y), %xmm3
	addps	%xmm5,   %xmm3
	movaps	%xmm3, -20 * SIZE(Y)
	movaps	 -4 * SIZE(X), %xmm3

	movss	 %xmm1,  %xmm0
	shufps	 $0x93,  %xmm1, %xmm0
	PSHUFD2($0xb1,   %xmm0, %xmm5)
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm5
	addps	-16 * SIZE(Y), %xmm0
	addps	%xmm5,   %xmm0
	movaps	%xmm0, -16 * SIZE(Y)
	movaps	  0 * SIZE(X), %xmm0

	movss	 %xmm2,  %xmm1
	shufps	 $0x93,  %xmm2, %xmm1
	PSHUFD2($0xb1,   %xmm1, %xmm5)
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm5
	addps	-12 * SIZE(Y), %xmm1
	addps	%xmm5,   %xmm1
	movaps	%xmm1, -12 * SIZE(Y)

	movss	 %xmm3,  %xmm2
	shufps	 $0x93,  %xmm3, %xmm2
	PSHUFD2($0xb1,   %xmm2, %xmm5)
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm5
	addps	 -8 * SIZE(Y), %xmm2
	addps	%xmm5,   %xmm2
	movaps	%xmm2,  -8 * SIZE(Y)

	movss	 %xmm0,  %xmm3
	shufps	 $0x93,  %xmm0, %xmm3
	PSHUFD2($0xb1,   %xmm3, %xmm5)
	mulps	ALPHA_R, %xmm3
	mulps	ALPHA_I, %xmm5
	addps	 -4 * SIZE(Y), %xmm3
	addps	%xmm5,   %xmm3
	movaps	%xmm3,  -4 * SIZE(Y)

	subl	$-32 * SIZE, X
	subl	$-32 * SIZE, Y
	ALIGN_3

.L45:
	testl	$8, M
	jle	.L46

	movaps	-28 * SIZE(X), %xmm1
	movaps	-24 * SIZE(X), %xmm2

	movss	 %xmm1,  %xmm0
	shufps	 $0x93,  %xmm1, %xmm0
	PSHUFD2($0xb1,   %xmm0, %xmm5)
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm5
	addps	-32 * SIZE(Y), %xmm0
	addps	%xmm5,   %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	movss	 %xmm2,  %xmm1
	shufps	 $0x93,  %xmm2, %xmm1
	PSHUFD2($0xb1,   %xmm1, %xmm5)
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm5
	addps	-28 * SIZE(Y), %xmm1
	addps	%xmm5,   %xmm1
	movaps	%xmm1, -28 * SIZE(Y)

	movaps	-20 * SIZE(X), %xmm3
	movaps	-16 * SIZE(X), %xmm0

	movss	 %xmm3,  %xmm2
	shufps	 $0x93,  %xmm3, %xmm2
	PSHUFD2($0xb1,   %xmm2, %xmm5)
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm5
	addps	-24 * SIZE(Y), %xmm2
	addps	%xmm5,   %xmm2
	movaps	%xmm2, -24 * SIZE(Y)

	movss	 %xmm0,  %xmm3
	shufps	 $0x93,  %xmm0, %xmm3
	PSHUFD2($0xb1,   %xmm3, %xmm5)
	mulps	ALPHA_R, %xmm3
	mulps	ALPHA_I, %xmm5
	addps	-20 * SIZE(Y), %xmm3
	addps	%xmm5,   %xmm3
	movaps	%xmm3, -20 * SIZE(Y)

	addl	$16 * SIZE, X
	addl	$16 * SIZE, Y
	ALIGN_2

.L46:
	testl	$4, M
	jle	.L47

	movaps	-28 * SIZE(X), %xmm1
	movaps	-24 * SIZE(X), %xmm2

	movss	 %xmm1,  %xmm0
	shufps	 $0x93,  %xmm1, %xmm0
	PSHUFD2($0xb1,   %xmm0, %xmm5)
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm5
	addps	-32 * SIZE(Y), %xmm0
	addps	%xmm5,   %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	movss	 %xmm2,  %xmm1
	shufps	 $0x93,  %xmm2, %xmm1
	PSHUFD2($0xb1,   %xmm1, %xmm5)
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm5
	addps	-28 * SIZE(Y), %xmm1
	addps	%xmm5,   %xmm1
	movaps	%xmm1, -28 * SIZE(Y)

	movaps	%xmm2, %xmm0

	addl	$8 * SIZE, X
	addl	$8 * SIZE, Y
	ALIGN_2

.L47:
	testl	$2, M
	jle	.L48

	movaps	-28 * SIZE(X), %xmm1

	movss	 %xmm1,  %xmm0
	shufps	 $0x93,  %xmm1, %xmm0
	PSHUFD2($0xb1,   %xmm0, %xmm5)
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm5

	addps	-32 * SIZE(Y), %xmm0
	addps	%xmm5,   %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	movaps	%xmm1, %xmm0

	addl	$4 * SIZE, X
	addl	$4 * SIZE, Y
	ALIGN_2

.L48:
	testl	$1, M
	jle	.L999

	movaps	-28 * SIZE(X), %xmm1
	movsd	-32 * SIZE(Y), %xmm2

	movss	 %xmm1,  %xmm0
	shufps	 $0x93,  %xmm1, %xmm0
	PSHUFD2($0xb1,   %xmm0, %xmm5)
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm5

	addps	%xmm5,   %xmm0
	addps	%xmm2,   %xmm0
	movlps	%xmm0, -32 * SIZE(Y)

	jmp	.L999
	ALIGN_3
#endif

.L50:
	xorps	%xmm0, %xmm0

	subl	$1 * SIZE, Y

	testl	$3 * SIZE, X
	jne	.L60

	movl	M,  %eax
	sarl	$4, %eax
	jle	.L55

	movaps	-32 * SIZE(X), %xmm1
	movaps	-28 * SIZE(X), %xmm2
	movaps	-24 * SIZE(X), %xmm3

	decl	%eax
	jle	.L52
	ALIGN_3

.L51:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	PSHUFD2($0xb1,   %xmm1, %xmm5)
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm1
	movss	%xmm1,   %xmm0
	shufps	$0x93,   %xmm1, %xmm0
	addps	-32 * SIZE(Y), %xmm0
	movaps	%xmm0, -32 * SIZE(Y)
	movaps	-20 * SIZE(X), %xmm0

	PSHUFD2($0xb1,   %xmm2, %xmm5)
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm2
	movss	%xmm2,   %xmm1
	shufps	$0x93,   %xmm2, %xmm1
	addps	-28 * SIZE(Y), %xmm1
	movaps	%xmm1, -28 * SIZE(Y)
	movaps	-16 * SIZE(X), %xmm1

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	PSHUFD2($0xb1,   %xmm3, %xmm5)
	mulps	ALPHA_R, %xmm3
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm3
	movss	%xmm3,   %xmm2
	shufps	$0x93,   %xmm3, %xmm2
	addps	-24 * SIZE(Y), %xmm2
	movaps	%xmm2, -24 * SIZE(Y)
	movaps	-12 * SIZE(X), %xmm2

	PSHUFD2($0xb1,   %xmm0, %xmm5)
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm0
	movss	%xmm0,   %xmm3
	shufps	$0x93,   %xmm0, %xmm3
	addps	-20 * SIZE(Y), %xmm3
	movaps	%xmm3, -20 * SIZE(Y)
	movaps	 -8 * SIZE(X), %xmm3

#if defined(PREFETCHW) && !defined(FETCH128)
	PREFETCHW (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	PSHUFD2($0xb1,   %xmm1, %xmm5)
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm1
	movss	%xmm1,   %xmm0
	shufps	$0x93,   %xmm1, %xmm0
	addps	-16 * SIZE(Y), %xmm0
	movaps	%xmm0, -16 * SIZE(Y)
	movaps	 -4 * SIZE(X), %xmm0

	PSHUFD2($0xb1,   %xmm2, %xmm5)
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm2
	movss	%xmm2,   %xmm1
	shufps	$0x93,   %xmm2, %xmm1
	addps	-12 * SIZE(Y), %xmm1
	movaps	%xmm1, -12 * SIZE(Y)
	movaps	  0 * SIZE(X), %xmm1

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	PSHUFD2($0xb1,   %xmm3, %xmm5)
	mulps	ALPHA_R, %xmm3
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm3
	movss	%xmm3,   %xmm2
	shufps	$0x93,   %xmm3, %xmm2
	addps	 -8 * SIZE(Y), %xmm2
	movaps	%xmm2,  -8 * SIZE(Y)
	movaps	  4 * SIZE(X), %xmm2

	PSHUFD2($0xb1,   %xmm0, %xmm5)
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm0
	movss	%xmm0,   %xmm3
	shufps	$0x93,   %xmm0, %xmm3
	addps	 -4 * SIZE(Y), %xmm3
	movaps	%xmm3,  -4 * SIZE(Y)
	movaps	  8 * SIZE(X), %xmm3

	subl	$-32 * SIZE, X
	subl	$-32 * SIZE, Y
	decl	%eax
	jg	.L51
	ALIGN_3

.L52:
	PSHUFD2($0xb1,   %xmm1, %xmm5)
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm1
	movss	%xmm1,   %xmm0
	shufps	$0x93,   %xmm1, %xmm0
	addps	-32 * SIZE(Y), %xmm0
	movaps	%xmm0, -32 * SIZE(Y)
	movaps	-20 * SIZE(X), %xmm0

	PSHUFD2($0xb1,   %xmm2, %xmm5)
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm2
	movss	%xmm2,   %xmm1
	shufps	$0x93,   %xmm2, %xmm1
	addps	-28 * SIZE(Y), %xmm1
	movaps	%xmm1, -28 * SIZE(Y)
	movaps	-16 * SIZE(X), %xmm1

	PSHUFD2($0xb1,   %xmm3, %xmm5)
	mulps	ALPHA_R, %xmm3
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm3
	movss	%xmm3,   %xmm2
	shufps	$0x93,   %xmm3, %xmm2
	addps	-24 * SIZE(Y), %xmm2
	movaps	%xmm2, -24 * SIZE(Y)
	movaps	-12 * SIZE(X), %xmm2

	PSHUFD2($0xb1,   %xmm0, %xmm5)
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm0
	movss	%xmm0,   %xmm3
	shufps	$0x93,   %xmm0, %xmm3
	addps	-20 * SIZE(Y), %xmm3
	movaps	%xmm3, -20 * SIZE(Y)
	movaps	 -8 * SIZE(X), %xmm3

	PSHUFD2($0xb1,   %xmm1, %xmm5)
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm1
	movss	%xmm1,   %xmm0
	shufps	$0x93,   %xmm1, %xmm0
	addps	-16 * SIZE(Y), %xmm0
	movaps	%xmm0, -16 * SIZE(Y)
	movaps	 -4 * SIZE(X), %xmm0

	PSHUFD2($0xb1,   %xmm2, %xmm5)
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm2
	movss	%xmm2,   %xmm1
	shufps	$0x93,   %xmm2, %xmm1
	addps	-12 * SIZE(Y), %xmm1
	movaps	%xmm1, -12 * SIZE(Y)

	PSHUFD2($0xb1,   %xmm3, %xmm5)
	mulps	ALPHA_R, %xmm3
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm3
	movss	%xmm3,   %xmm2
	shufps	$0x93,   %xmm3, %xmm2
	addps	 -8 * SIZE(Y), %xmm2
	movaps	%xmm2,  -8 * SIZE(Y)

	PSHUFD2($0xb1,   %xmm0, %xmm5)
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm0
	movss	%xmm0,   %xmm3
	shufps	$0x93,   %xmm0, %xmm3
	addps	 -4 * SIZE(Y), %xmm3
	movaps	%xmm3,  -4 * SIZE(Y)

	subl	$-32 * SIZE, X
	subl	$-32 * SIZE, Y
	ALIGN_3

.L55:
	testl	$8, M
	jle	.L56

	movaps	-32 * SIZE(X), %xmm1
	movaps	-28 * SIZE(X), %xmm2

	PSHUFD2($0xb1,   %xmm1, %xmm5)
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm1
	movss	%xmm1,   %xmm0
	shufps	$0x93,   %xmm1, %xmm0
	addps	-32 * SIZE(Y), %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	PSHUFD2($0xb1,   %xmm2, %xmm5)
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm2
	movss	%xmm2,   %xmm1
	shufps	$0x93,   %xmm2, %xmm1
	addps	-28 * SIZE(Y), %xmm1
	movaps	%xmm1, -28 * SIZE(Y)

	movaps	-24 * SIZE(X), %xmm3
	movaps	-20 * SIZE(X), %xmm0

	PSHUFD2($0xb1,   %xmm3, %xmm5)
	mulps	ALPHA_R, %xmm3
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm3
	movss	%xmm3,   %xmm2
	shufps	$0x93,   %xmm3, %xmm2
	addps	-24 * SIZE(Y), %xmm2
	movaps	%xmm2, -24 * SIZE(Y)

	PSHUFD2($0xb1,   %xmm0, %xmm5)
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm0
	movss	%xmm0,   %xmm3
	shufps	$0x93,   %xmm0, %xmm3
	addps	-20 * SIZE(Y), %xmm3
	movaps	%xmm3, -20 * SIZE(Y)

	addl	$16 * SIZE, X
	addl	$16 * SIZE, Y
	ALIGN_2

.L56:
	testl	$4, M
	jle	.L57

	movaps	-32 * SIZE(X), %xmm1
	movaps	-28 * SIZE(X), %xmm2

	PSHUFD2($0xb1,   %xmm1, %xmm5)
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm1
	movss	%xmm1,   %xmm0
	shufps	$0x93,   %xmm1, %xmm0
	addps	-32 * SIZE(Y), %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	PSHUFD2($0xb1,   %xmm2, %xmm5)
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm2
	movss	%xmm2,   %xmm1
	shufps	$0x93,   %xmm2, %xmm1
	addps	-28 * SIZE(Y), %xmm1
	movaps	%xmm1, -28 * SIZE(Y)

	movaps	%xmm2, %xmm0

	addl	$8 * SIZE, X
	addl	$8 * SIZE, Y
	ALIGN_2

.L57:
	testl	$2, M
	jle	.L58

	movaps	-32 * SIZE(X), %xmm1

	PSHUFD2($0xb1,   %xmm1, %xmm5)
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm1
	movss	%xmm1,   %xmm0
	shufps	$0x93,   %xmm1, %xmm0

	addps	-32 * SIZE(Y), %xmm0
	movaps	%xmm0, -32 * SIZE(Y)
	movaps	%xmm1, %xmm0

	addl	$4 * SIZE, X
	addl	$4 * SIZE, Y
	ALIGN_2

.L58:
	testl	$1, M
	jle	.L59

#ifdef movsd
	xorps	%xmm1, %xmm1
#endif
	movsd	-32 * SIZE(X), %xmm1

	PSHUFD2($0xb1,   %xmm1, %xmm5)
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm1
	movss	%xmm1,   %xmm0
	shufps	$0x93,   %xmm1, %xmm0

	addps	-32 * SIZE(Y), %xmm0
	movaps	%xmm0, -32 * SIZE(Y)
	jmp	.L999
	ALIGN_3

.L59:
	shufps	$0x93,  %xmm0, %xmm0

	addss	-32 * SIZE(Y), %xmm0
	movss	%xmm0, -32 * SIZE(Y)
	jmp	.L999
	ALIGN_3

.L60:
#ifdef ALIGNED_ACCESS

	testl	$2 * SIZE, X
	jne	.L70

	subl	$1 * SIZE, X

	movaps	-32 * SIZE(X), %xmm1

	movl	M,  %eax
	sarl	$4, %eax
	jle	.L65

	movaps	-28 * SIZE(X), %xmm2
	movaps	-24 * SIZE(X), %xmm3

	decl	%eax
	jle	.L62
	ALIGN_3

.L61:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	movss	 %xmm2,  %xmm1
	SHUFPS_39  %xmm1, %xmm1
	PSHUFD2($0xb1,   %xmm1, %xmm5)
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm1
	movss	%xmm1,   %xmm0
	shufps	$0x93,   %xmm1, %xmm0
	addps	-32 * SIZE(Y), %xmm0
	movaps	%xmm0, -32 * SIZE(Y)
	movaps	-20 * SIZE(X), %xmm0

	movss	 %xmm3,  %xmm2
	SHUFPS_39  %xmm2, %xmm2
	PSHUFD2($0xb1,   %xmm2, %xmm5)
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm2
	movss	%xmm2,   %xmm1
	shufps	$0x93,   %xmm2, %xmm1
	addps	-28 * SIZE(Y), %xmm1
	movaps	%xmm1, -28 * SIZE(Y)
	movaps	-16 * SIZE(X), %xmm1

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movss	 %xmm0,  %xmm3
	SHUFPS_39  %xmm3, %xmm3
	PSHUFD2($0xb1,   %xmm3, %xmm5)
	mulps	ALPHA_R, %xmm3
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm3
	movss	%xmm3,   %xmm2
	shufps	$0x93,   %xmm3, %xmm2
	addps	-24 * SIZE(Y), %xmm2
	movaps	%xmm2, -24 * SIZE(Y)
	movaps	-12 * SIZE(X), %xmm2

	movss	 %xmm1,  %xmm0
	SHUFPS_39  %xmm0, %xmm0
	PSHUFD2($0xb1,   %xmm0, %xmm5)
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm0
	movss	%xmm0,   %xmm3
	shufps	$0x93,   %xmm0, %xmm3
	addps	-20 * SIZE(Y), %xmm3
	movaps	%xmm3, -20 * SIZE(Y)
	movaps	 -8 * SIZE(X), %xmm3

#if defined(PREFETCHW) && !defined(FETCH128)
	PREFETCHW (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	movss	 %xmm2,  %xmm1
	SHUFPS_39  %xmm1, %xmm1
	PSHUFD2($0xb1,   %xmm1, %xmm5)
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm1
	movss	%xmm1,   %xmm0
	shufps	$0x93,   %xmm1, %xmm0
	addps	-16 * SIZE(Y), %xmm0
	movaps	%xmm0, -16 * SIZE(Y)
	movaps	 -4 * SIZE(X), %xmm0

	movss	 %xmm3,  %xmm2
	SHUFPS_39  %xmm2, %xmm2
	PSHUFD2($0xb1,   %xmm2, %xmm5)
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm2
	movss	%xmm2,   %xmm1
	shufps	$0x93,   %xmm2, %xmm1
	addps	-12 * SIZE(Y), %xmm1
	movaps	%xmm1, -12 * SIZE(Y)
	movaps	  0 * SIZE(X), %xmm1

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	movss	 %xmm0,  %xmm3
	SHUFPS_39  %xmm3, %xmm3
	PSHUFD2($0xb1,   %xmm3, %xmm5)
	mulps	ALPHA_R, %xmm3
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm3
	movss	%xmm3,   %xmm2
	shufps	$0x93,   %xmm3, %xmm2
	addps	 -8 * SIZE(Y), %xmm2
	movaps	%xmm2,  -8 * SIZE(Y)
	movaps	  4 * SIZE(X), %xmm2

	movss	 %xmm1,  %xmm0
	SHUFPS_39  %xmm0, %xmm0
	PSHUFD2($0xb1,   %xmm0, %xmm5)
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm0
	movss	%xmm0,   %xmm3
	shufps	$0x93,   %xmm0, %xmm3
	addps	 -4 * SIZE(Y), %xmm3
	movaps	%xmm3,  -4 * SIZE(Y)
	movaps	  8 * SIZE(X), %xmm3

	subl	$-32 * SIZE, X
	subl	$-32 * SIZE, Y
	decl	%eax
	jg	.L61
	ALIGN_3

.L62:
	movss	 %xmm2,  %xmm1
	SHUFPS_39  %xmm1, %xmm1
	PSHUFD2($0xb1,   %xmm1, %xmm5)
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm1
	movss	%xmm1,   %xmm0
	shufps	$0x93,   %xmm1, %xmm0
	addps	-32 * SIZE(Y), %xmm0
	movaps	%xmm0, -32 * SIZE(Y)
	movaps	-20 * SIZE(X), %xmm0

	movss	 %xmm3,  %xmm2
	SHUFPS_39  %xmm2, %xmm2
	PSHUFD2($0xb1,   %xmm2, %xmm5)
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm2
	movss	%xmm2,   %xmm1
	shufps	$0x93,   %xmm2, %xmm1
	addps	-28 * SIZE(Y), %xmm1
	movaps	%xmm1, -28 * SIZE(Y)
	movaps	-16 * SIZE(X), %xmm1

	movss	 %xmm0,  %xmm3
	SHUFPS_39  %xmm3, %xmm3
	PSHUFD2($0xb1,   %xmm3, %xmm5)
	mulps	ALPHA_R, %xmm3
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm3
	movss	%xmm3,   %xmm2
	shufps	$0x93,   %xmm3, %xmm2
	addps	-24 * SIZE(Y), %xmm2
	movaps	%xmm2, -24 * SIZE(Y)
	movaps	-12 * SIZE(X), %xmm2

	movss	 %xmm1,  %xmm0
	SHUFPS_39  %xmm0, %xmm0
	PSHUFD2($0xb1,   %xmm0, %xmm5)
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm0
	movss	%xmm0,   %xmm3
	shufps	$0x93,   %xmm0, %xmm3
	addps	-20 * SIZE(Y), %xmm3
	movaps	%xmm3, -20 * SIZE(Y)
	movaps	 -8 * SIZE(X), %xmm3

	movss	 %xmm2,  %xmm1
	SHUFPS_39  %xmm1, %xmm1
	PSHUFD2($0xb1,   %xmm1, %xmm5)
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm1
	movss	%xmm1,   %xmm0
	shufps	$0x93,   %xmm1, %xmm0
	addps	-16 * SIZE(Y), %xmm0
	movaps	%xmm0, -16 * SIZE(Y)
	movaps	 -4 * SIZE(X), %xmm0

	movss	 %xmm3,  %xmm2
	SHUFPS_39  %xmm2, %xmm2
	PSHUFD2($0xb1,   %xmm2, %xmm5)
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm2
	movss	%xmm2,   %xmm1
	shufps	$0x93,   %xmm2, %xmm1
	addps	-12 * SIZE(Y), %xmm1
	movaps	%xmm1, -12 * SIZE(Y)
	movaps	  0 * SIZE(X), %xmm1

	movss	 %xmm0,  %xmm3
	SHUFPS_39  %xmm3, %xmm3
	PSHUFD2($0xb1,   %xmm3, %xmm5)
	mulps	ALPHA_R, %xmm3
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm3
	movss	%xmm3,   %xmm2
	shufps	$0x93,   %xmm3, %xmm2
	addps	 -8 * SIZE(Y), %xmm2
	movaps	%xmm2,  -8 * SIZE(Y)

	movss	 %xmm1,  %xmm0
	SHUFPS_39  %xmm0, %xmm0
	PSHUFD2($0xb1,   %xmm0, %xmm5)
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm0
	movss	%xmm0,   %xmm3
	shufps	$0x93,   %xmm0, %xmm3
	addps	 -4 * SIZE(Y), %xmm3
	movaps	%xmm3,  -4 * SIZE(Y)

	subl	$-32 * SIZE, X
	subl	$-32 * SIZE, Y
	ALIGN_3

.L65:
	testl	$8, M
	jle	.L66

	movaps	-28 * SIZE(X), %xmm2
	movaps	-24 * SIZE(X), %xmm3

	movss	 %xmm2,  %xmm1
	SHUFPS_39  %xmm1, %xmm1
	PSHUFD2($0xb1,   %xmm1, %xmm5)
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm1
	movss	%xmm1,   %xmm0
	shufps	$0x93,   %xmm1, %xmm0
	addps	-32 * SIZE(Y), %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	movss	 %xmm3,  %xmm2
	SHUFPS_39  %xmm2, %xmm2
	PSHUFD2($0xb1,   %xmm2, %xmm5)
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm2
	movss	%xmm2,   %xmm1
	shufps	$0x93,   %xmm2, %xmm1
	addps	-28 * SIZE(Y), %xmm1
	movaps	%xmm1, -28 * SIZE(Y)

	movaps	-20 * SIZE(X), %xmm0
	movaps	-16 * SIZE(X), %xmm1

	movss	 %xmm0,  %xmm3
	SHUFPS_39  %xmm3, %xmm3
	PSHUFD2($0xb1,   %xmm3, %xmm5)
	mulps	ALPHA_R, %xmm3
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm3
	movss	%xmm3,   %xmm2
	shufps	$0x93,   %xmm3, %xmm2
	addps	-24 * SIZE(Y), %xmm2
	movaps	%xmm2, -24 * SIZE(Y)

	movss	 %xmm1,  %xmm0
	SHUFPS_39  %xmm0, %xmm0
	PSHUFD2($0xb1,   %xmm0, %xmm5)
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm0
	movss	%xmm0,   %xmm3
	shufps	$0x93,   %xmm0, %xmm3
	addps	-20 * SIZE(Y), %xmm3
	movaps	%xmm3, -20 * SIZE(Y)

	addl	$16 * SIZE, X
	addl	$16 * SIZE, Y
	ALIGN_2

.L66:
	testl	$4, M
	jle	.L67

	movaps	-28 * SIZE(X), %xmm2
	movaps	-24 * SIZE(X), %xmm3

	movss	 %xmm2,  %xmm1
	SHUFPS_39  %xmm1, %xmm1
	PSHUFD2($0xb1,   %xmm1, %xmm5)
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm1
	movss	%xmm1,   %xmm0
	shufps	$0x93,   %xmm1, %xmm0
	addps	-32 * SIZE(Y), %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	movss	 %xmm3,  %xmm2
	SHUFPS_39  %xmm2, %xmm2
	PSHUFD2($0xb1,   %xmm2, %xmm5)
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm2
	movss	%xmm2,   %xmm1
	shufps	$0x93,   %xmm2, %xmm1
	addps	-28 * SIZE(Y), %xmm1
	movaps	%xmm1, -28 * SIZE(Y)

	movaps	%xmm2, %xmm0
	movaps	%xmm3, %xmm1

	addl	$8 * SIZE, X
	addl	$8 * SIZE, Y
	ALIGN_2

.L67:
	testl	$2, M
	jle	.L68

	movaps	-28 * SIZE(X), %xmm2

	movss	 %xmm2,  %xmm1
	SHUFPS_39  %xmm1, %xmm1
	PSHUFD2($0xb1,   %xmm1, %xmm5)
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm1
	movss	%xmm1,   %xmm0
	shufps	$0x93,   %xmm1, %xmm0
	addps	-32 * SIZE(Y), %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	movaps	%xmm1, %xmm0
	movaps	%xmm2, %xmm1

	addl	$4 * SIZE, X
	addl	$4 * SIZE, Y
	ALIGN_2

.L68:
	testl	$1, M
	jle	.L69

	movaps	-28 * SIZE(X), %xmm2

	movss	 %xmm2,  %xmm1
	SHUFPS_39  %xmm1, %xmm1
	PSHUFD2($0xb1,   %xmm1, %xmm5)
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm1
	movss	%xmm1,   %xmm0
	shufps	$0x93,   %xmm1, %xmm0

	addps	-32 * SIZE(Y), %xmm0
	movlps	%xmm0, -32 * SIZE(Y)
	movhlps	%xmm0, %xmm0
	movss	%xmm0, -30 * SIZE(Y)
	jmp	.L999

.L69:
	shufps	$0x93,  %xmm0, %xmm0

	addss	-32 * SIZE(Y), %xmm0
	movss	%xmm0, -32 * SIZE(Y)
	jmp	.L999
	ALIGN_3

.L70:
	testl	$1 * SIZE, X
	jne	.L80
#endif

	movl	M,  %eax
	sarl	$4, %eax
	jle	.L75

	movsd	-32 * SIZE(X), %xmm1
	movhps	-30 * SIZE(X), %xmm1
	movsd	-28 * SIZE(X), %xmm2
	movhps	-26 * SIZE(X), %xmm2
	movsd	-24 * SIZE(X), %xmm3
	movhps	-22 * SIZE(X), %xmm3

	decl	%eax
	jle	.L72
	ALIGN_3

.L71:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	PSHUFD2($0xb1,   %xmm1, %xmm5)
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm1
	movss	%xmm1,   %xmm0
	shufps	$0x93,   %xmm1, %xmm0
	addps	-32 * SIZE(Y), %xmm0
	movaps	%xmm0, -32 * SIZE(Y)
	movsd	-20 * SIZE(X), %xmm0
	movhps	-18 * SIZE(X), %xmm0

	PSHUFD2($0xb1,   %xmm2, %xmm5)
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm2
	movss	%xmm2,   %xmm1
	shufps	$0x93,   %xmm2, %xmm1
	addps	-28 * SIZE(Y), %xmm1
	movaps	%xmm1, -28 * SIZE(Y)
	movsd	-16 * SIZE(X), %xmm1
	movhps	-14 * SIZE(X), %xmm1

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	PSHUFD2($0xb1,   %xmm3, %xmm5)
	mulps	ALPHA_R, %xmm3
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm3
	movss	%xmm3,   %xmm2
	shufps	$0x93,   %xmm3, %xmm2
	addps	-24 * SIZE(Y), %xmm2
	movaps	%xmm2, -24 * SIZE(Y)
	movsd	-12 * SIZE(X), %xmm2
	movhps	-10 * SIZE(X), %xmm2

	PSHUFD2($0xb1,   %xmm0, %xmm5)
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm0
	movss	%xmm0,   %xmm3
	shufps	$0x93,   %xmm0, %xmm3
	addps	-20 * SIZE(Y), %xmm3
	movaps	%xmm3, -20 * SIZE(Y)
	movsd	 -8 * SIZE(X), %xmm3
	movhps	 -6 * SIZE(X), %xmm3

#if defined(PREFETCHW) && !defined(FETCH128)
	PREFETCHW (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	PSHUFD2($0xb1,   %xmm1, %xmm5)
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm1
	movss	%xmm1,   %xmm0
	shufps	$0x93,   %xmm1, %xmm0
	addps	-16 * SIZE(Y), %xmm0
	movaps	%xmm0, -16 * SIZE(Y)
	movsd	 -4 * SIZE(X), %xmm0
	movhps	 -2 * SIZE(X), %xmm0

	PSHUFD2($0xb1,   %xmm2, %xmm5)
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm2
	movss	%xmm2,   %xmm1
	shufps	$0x93,   %xmm2, %xmm1
	addps	-12 * SIZE(Y), %xmm1
	movaps	%xmm1, -12 * SIZE(Y)
	movsd	  0 * SIZE(X), %xmm1
	movhps	  2 * SIZE(X), %xmm1

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	PSHUFD2($0xb1,   %xmm3, %xmm5)
	mulps	ALPHA_R, %xmm3
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm3
	movss	%xmm3,   %xmm2
	shufps	$0x93,   %xmm3, %xmm2
	addps	 -8 * SIZE(Y), %xmm2
	movaps	%xmm2,  -8 * SIZE(Y)
	movsd	  4 * SIZE(X), %xmm2
	movhps	  6 * SIZE(X), %xmm2

	PSHUFD2($0xb1,   %xmm0, %xmm5)
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm0
	movss	%xmm0,   %xmm3
	shufps	$0x93,   %xmm0, %xmm3
	addps	 -4 * SIZE(Y), %xmm3
	movaps	%xmm3,  -4 * SIZE(Y)
	movsd	  8 * SIZE(X), %xmm3
	movhps	 10 * SIZE(X), %xmm3

	subl	$-32 * SIZE, X
	subl	$-32 * SIZE, Y
	decl	%eax
	jg	.L71
	ALIGN_3

.L72:
	PSHUFD2($0xb1,   %xmm1, %xmm5)
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm1
	movss	%xmm1,   %xmm0
	shufps	$0x93,   %xmm1, %xmm0
	addps	-32 * SIZE(Y), %xmm0
	movaps	%xmm0, -32 * SIZE(Y)
	movsd	-20 * SIZE(X), %xmm0
	movhps	-18 * SIZE(X), %xmm0

	PSHUFD2($0xb1,   %xmm2, %xmm5)
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm2
	movss	%xmm2,   %xmm1
	shufps	$0x93,   %xmm2, %xmm1
	addps	-28 * SIZE(Y), %xmm1
	movaps	%xmm1, -28 * SIZE(Y)
	movsd	-16 * SIZE(X), %xmm1
	movhps	-14 * SIZE(X), %xmm1

	PSHUFD2($0xb1,   %xmm3, %xmm5)
	mulps	ALPHA_R, %xmm3
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm3
	movss	%xmm3,   %xmm2
	shufps	$0x93,   %xmm3, %xmm2
	addps	-24 * SIZE(Y), %xmm2
	movaps	%xmm2, -24 * SIZE(Y)
	movsd	-12 * SIZE(X), %xmm2
	movhps	-10 * SIZE(X), %xmm2

	PSHUFD2($0xb1,   %xmm0, %xmm5)
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm0
	movss	%xmm0,   %xmm3
	shufps	$0x93,   %xmm0, %xmm3
	addps	-20 * SIZE(Y), %xmm3
	movaps	%xmm3, -20 * SIZE(Y)
	movsd	 -8 * SIZE(X), %xmm3
	movhps	 -6 * SIZE(X), %xmm3

	PSHUFD2($0xb1,   %xmm1, %xmm5)
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm1
	movss	%xmm1,   %xmm0
	shufps	$0x93,   %xmm1, %xmm0
	addps	-16 * SIZE(Y), %xmm0
	movaps	%xmm0, -16 * SIZE(Y)
	movsd	 -4 * SIZE(X), %xmm0
	movhps	 -2 * SIZE(X), %xmm0

	PSHUFD2($0xb1,   %xmm2, %xmm5)
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm2
	movss	%xmm2,   %xmm1
	shufps	$0x93,   %xmm2, %xmm1
	addps	-12 * SIZE(Y), %xmm1
	movaps	%xmm1, -12 * SIZE(Y)

	PSHUFD2($0xb1,   %xmm3, %xmm5)
	mulps	ALPHA_R, %xmm3
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm3
	movss	%xmm3,   %xmm2
	shufps	$0x93,   %xmm3, %xmm2
	addps	 -8 * SIZE(Y), %xmm2
	movaps	%xmm2,  -8 * SIZE(Y)

	PSHUFD2($0xb1,   %xmm0, %xmm5)
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm0
	movss	%xmm0,   %xmm3
	shufps	$0x93,   %xmm0, %xmm3
	addps	 -4 * SIZE(Y), %xmm3
	movaps	%xmm3,  -4 * SIZE(Y)

	subl	$-32 * SIZE, X
	subl	$-32 * SIZE, Y
	ALIGN_3

.L75:
	testl	$8, M
	jle	.L76

	movsd	-32 * SIZE(X), %xmm1
	movhps	-30 * SIZE(X), %xmm1
	movsd	-28 * SIZE(X), %xmm2
	movhps	-26 * SIZE(X), %xmm2

	PSHUFD2($0xb1,   %xmm1, %xmm5)
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm1
	movss	%xmm1,   %xmm0
	shufps	$0x93,   %xmm1, %xmm0
	addps	-32 * SIZE(Y), %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	PSHUFD2($0xb1,   %xmm2, %xmm5)
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm2
	movss	%xmm2,   %xmm1
	shufps	$0x93,   %xmm2, %xmm1
	addps	-28 * SIZE(Y), %xmm1
	movaps	%xmm1, -28 * SIZE(Y)

	movsd	-24 * SIZE(X), %xmm3
	movhps	-22 * SIZE(X), %xmm3
	movsd	-20 * SIZE(X), %xmm0
	movhps	-18 * SIZE(X), %xmm0

	PSHUFD2($0xb1,   %xmm3, %xmm5)
	mulps	ALPHA_R, %xmm3
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm3
	movss	%xmm3,   %xmm2
	shufps	$0x93,   %xmm3, %xmm2
	addps	-24 * SIZE(Y), %xmm2
	movaps	%xmm2, -24 * SIZE(Y)

	PSHUFD2($0xb1,   %xmm0, %xmm5)
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm0
	movss	%xmm0,   %xmm3
	shufps	$0x93,   %xmm0, %xmm3
	addps	-20 * SIZE(Y), %xmm3
	movaps	%xmm3, -20 * SIZE(Y)

	addl	$16 * SIZE, X
	addl	$16 * SIZE, Y
	ALIGN_2

.L76:
	testl	$4, M
	jle	.L77

	movsd	-32 * SIZE(X), %xmm1
	movhps	-30 * SIZE(X), %xmm1

	PSHUFD2($0xb1,   %xmm1, %xmm5)
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm1
	movss	%xmm1,   %xmm0
	shufps	$0x93,   %xmm1, %xmm0
	addps	-32 * SIZE(Y), %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	movsd	-28 * SIZE(X), %xmm2
	movhps	-26 * SIZE(X), %xmm2

	PSHUFD2($0xb1,   %xmm2, %xmm5)
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm2
	movss	%xmm2,   %xmm1
	shufps	$0x93,   %xmm2, %xmm1
	addps	-28 * SIZE(Y), %xmm1
	movaps	%xmm1, -28 * SIZE(Y)

	movaps	%xmm2, %xmm0

	addl	$8 * SIZE, X
	addl	$8 * SIZE, Y
	ALIGN_2

.L77:
	testl	$2, M
	jle	.L78

	movsd	-32 * SIZE(X), %xmm1
	movhps	-30 * SIZE(X), %xmm1

	PSHUFD2($0xb1,   %xmm1, %xmm5)
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm1
	movss	%xmm1,   %xmm0
	shufps	$0x93,   %xmm1, %xmm0

	addps	-32 * SIZE(Y), %xmm0
	movaps	%xmm0, -32 * SIZE(Y)
	movaps	%xmm1, %xmm0

	addl	$4 * SIZE, X
	addl	$4 * SIZE, Y
	ALIGN_2

.L78:
	testl	$1, M
	jle	.L79

#ifdef movsd
	xorps	%xmm1, %xmm1
#endif
	movsd	-32 * SIZE(X), %xmm1

	PSHUFD2($0xb1,   %xmm1, %xmm5)
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm1
	movss	%xmm1,   %xmm0
	shufps	$0x93,   %xmm1, %xmm0

	addps	-32 * SIZE(Y), %xmm0
	movaps	%xmm0, -32 * SIZE(Y)
	jmp	.L999
	ALIGN_3

.L79:
	shufps	$0x93,  %xmm0, %xmm0

	addss	-32 * SIZE(Y), %xmm0
	movss	%xmm0, -32 * SIZE(Y)
	jmp	.L999
	ALIGN_3

#ifdef ALIGNED_ACCESS

.L80:
	subl	$3 * SIZE, X

	movaps	-32 * SIZE(X), %xmm1

	movl	M,  %eax
	sarl	$4, %eax
	jle	.L85

	movaps	-28 * SIZE(X), %xmm2
	movaps	-24 * SIZE(X), %xmm3

	decl	%eax
	jle	.L82
	ALIGN_3

.L81:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	movss	 %xmm2,  %xmm1
	shufps	 $0x93,  %xmm2, %xmm1
	PSHUFD2($0xb1,   %xmm1, %xmm5)
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm1
	movss	%xmm1,   %xmm0
	shufps	$0x93,   %xmm1, %xmm0
	addps	-32 * SIZE(Y), %xmm0
	movaps	%xmm0, -32 * SIZE(Y)
	movaps	-20 * SIZE(X), %xmm0

	movss	 %xmm3,  %xmm2
	shufps	 $0x93,  %xmm3, %xmm2
	PSHUFD2($0xb1,   %xmm2, %xmm5)
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm2
	movss	%xmm2,   %xmm1
	shufps	$0x93,   %xmm2, %xmm1
	addps	-28 * SIZE(Y), %xmm1
	movaps	%xmm1, -28 * SIZE(Y)
	movaps	-16 * SIZE(X), %xmm1

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movss	 %xmm0,  %xmm3
	shufps	 $0x93,  %xmm0, %xmm3
	PSHUFD2($0xb1,   %xmm3, %xmm5)
	mulps	ALPHA_R, %xmm3
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm3
	movss	%xmm3,   %xmm2
	shufps	$0x93,   %xmm3, %xmm2
	addps	-24 * SIZE(Y), %xmm2
	movaps	%xmm2, -24 * SIZE(Y)
	movaps	-12 * SIZE(X), %xmm2

	movss	 %xmm1,  %xmm0
	shufps	 $0x93,  %xmm1, %xmm0
	PSHUFD2($0xb1,   %xmm0, %xmm5)
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm0
	movss	%xmm0,   %xmm3
	shufps	$0x93,   %xmm0, %xmm3
	addps	-20 * SIZE(Y), %xmm3
	movaps	%xmm3, -20 * SIZE(Y)
	movaps	 -8 * SIZE(X), %xmm3

#if defined(PREFETCHW) && !defined(FETCH128)
	PREFETCHW (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	movss	 %xmm2,  %xmm1
	shufps	 $0x93,  %xmm2, %xmm1
	PSHUFD2($0xb1,   %xmm1, %xmm5)
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm1
	movss	%xmm1,   %xmm0
	shufps	$0x93,   %xmm1, %xmm0
	addps	-16 * SIZE(Y), %xmm0
	movaps	%xmm0, -16 * SIZE(Y)
	movaps	 -4 * SIZE(X), %xmm0

	movss	 %xmm3,  %xmm2
	shufps	 $0x93,  %xmm3, %xmm2
	PSHUFD2($0xb1,   %xmm2, %xmm5)
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm2
	movss	%xmm2,   %xmm1
	shufps	$0x93,   %xmm2, %xmm1
	addps	-12 * SIZE(Y), %xmm1
	movaps	%xmm1, -12 * SIZE(Y)
	movaps	  0 * SIZE(X), %xmm1

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	movss	 %xmm0,  %xmm3
	shufps	 $0x93,  %xmm0, %xmm3
	PSHUFD2($0xb1,   %xmm3, %xmm5)
	mulps	ALPHA_R, %xmm3
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm3
	movss	%xmm3,   %xmm2
	shufps	$0x93,   %xmm3, %xmm2
	addps	 -8 * SIZE(Y), %xmm2
	movaps	%xmm2,  -8 * SIZE(Y)
	movaps	  4 * SIZE(X), %xmm2

	movss	 %xmm1,  %xmm0
	shufps	 $0x93,  %xmm1, %xmm0
	PSHUFD2($0xb1,   %xmm0, %xmm5)
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm0
	movss	%xmm0,   %xmm3
	shufps	$0x93,   %xmm0, %xmm3
	addps	 -4 * SIZE(Y), %xmm3
	movaps	%xmm3,  -4 * SIZE(Y)
	movaps	  8 * SIZE(X), %xmm3

	subl	$-32 * SIZE, X
	subl	$-32 * SIZE, Y
	decl	%eax
	jg	.L81
	ALIGN_3

.L82:
	movss	 %xmm2,  %xmm1
	shufps	 $0x93,  %xmm2, %xmm1
	PSHUFD2($0xb1,   %xmm1, %xmm5)
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm1
	movss	%xmm1,   %xmm0
	shufps	$0x93,   %xmm1, %xmm0
	addps	-32 * SIZE(Y), %xmm0
	movaps	%xmm0, -32 * SIZE(Y)
	movaps	-20 * SIZE(X), %xmm0

	movss	 %xmm3,  %xmm2
	shufps	 $0x93,  %xmm3, %xmm2
	PSHUFD2($0xb1,   %xmm2, %xmm5)
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm2
	movss	%xmm2,   %xmm1
	shufps	$0x93,   %xmm2, %xmm1
	addps	-28 * SIZE(Y), %xmm1
	movaps	%xmm1, -28 * SIZE(Y)
	movaps	-16 * SIZE(X), %xmm1

	movss	 %xmm0,  %xmm3
	shufps	 $0x93,  %xmm0, %xmm3
	PSHUFD2($0xb1,   %xmm3, %xmm5)
	mulps	ALPHA_R, %xmm3
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm3
	movss	%xmm3,   %xmm2
	shufps	$0x93,   %xmm3, %xmm2
	addps	-24 * SIZE(Y), %xmm2
	movaps	%xmm2, -24 * SIZE(Y)
	movaps	-12 * SIZE(X), %xmm2

	movss	 %xmm1,  %xmm0
	shufps	 $0x93,  %xmm1, %xmm0
	PSHUFD2($0xb1,   %xmm0, %xmm5)
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm0
	movss	%xmm0,   %xmm3
	shufps	$0x93,   %xmm0, %xmm3
	addps	-20 * SIZE(Y), %xmm3
	movaps	%xmm3, -20 * SIZE(Y)
	movaps	 -8 * SIZE(X), %xmm3

	movss	 %xmm2,  %xmm1
	shufps	 $0x93,  %xmm2, %xmm1
	PSHUFD2($0xb1,   %xmm1, %xmm5)
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm1
	movss	%xmm1,   %xmm0
	shufps	$0x93,   %xmm1, %xmm0
	addps	-16 * SIZE(Y), %xmm0
	movaps	%xmm0, -16 * SIZE(Y)
	movaps	 -4 * SIZE(X), %xmm0

	movss	 %xmm3,  %xmm2
	shufps	 $0x93,  %xmm3, %xmm2
	PSHUFD2($0xb1,   %xmm2, %xmm5)
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm2
	movss	%xmm2,   %xmm1
	shufps	$0x93,   %xmm2, %xmm1
	addps	-12 * SIZE(Y), %xmm1
	movaps	%xmm1, -12 * SIZE(Y)
	movaps	  0 * SIZE(X), %xmm1

	movss	 %xmm0,  %xmm3
	shufps	 $0x93,  %xmm0, %xmm3
	PSHUFD2($0xb1,   %xmm3, %xmm5)
	mulps	ALPHA_R, %xmm3
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm3
	movss	%xmm3,   %xmm2
	shufps	$0x93,   %xmm3, %xmm2
	addps	 -8 * SIZE(Y), %xmm2
	movaps	%xmm2,  -8 * SIZE(Y)

	movss	 %xmm1,  %xmm0
	shufps	 $0x93,  %xmm1, %xmm0
	PSHUFD2($0xb1,   %xmm0, %xmm5)
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm0
	movss	%xmm0,   %xmm3
	shufps	$0x93,   %xmm0, %xmm3
	addps	 -4 * SIZE(Y), %xmm3
	movaps	%xmm3,  -4 * SIZE(Y)

	subl	$-32 * SIZE, X
	subl	$-32 * SIZE, Y
	ALIGN_3

.L85:
	testl	$8, M
	jle	.L86

	movaps	-28 * SIZE(X), %xmm2
	movaps	-24 * SIZE(X), %xmm3

	movss	 %xmm2,  %xmm1
	shufps	 $0x93,  %xmm2, %xmm1
	PSHUFD2($0xb1,   %xmm1, %xmm5)
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm1
	movss	%xmm1,   %xmm0
	shufps	$0x93,   %xmm1, %xmm0
	addps	-32 * SIZE(Y), %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	movss	 %xmm3,  %xmm2
	shufps	 $0x93,  %xmm3, %xmm2
	PSHUFD2($0xb1,   %xmm2, %xmm5)
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm2
	movss	%xmm2,   %xmm1
	shufps	$0x93,   %xmm2, %xmm1
	addps	-28 * SIZE(Y), %xmm1
	movaps	%xmm1, -28 * SIZE(Y)

	movaps	-20 * SIZE(X), %xmm0
	movaps	-16 * SIZE(X), %xmm1

	movss	 %xmm0,  %xmm3
	shufps	 $0x93,  %xmm0, %xmm3
	PSHUFD2($0xb1,   %xmm3, %xmm5)
	mulps	ALPHA_R, %xmm3
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm3
	movss	%xmm3,   %xmm2
	shufps	$0x93,   %xmm3, %xmm2
	addps	-24 * SIZE(Y), %xmm2
	movaps	%xmm2, -24 * SIZE(Y)

	movss	 %xmm1,  %xmm0
	shufps	 $0x93,  %xmm1, %xmm0
	PSHUFD2($0xb1,   %xmm0, %xmm5)
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm0
	movss	%xmm0,   %xmm3
	shufps	$0x93,   %xmm0, %xmm3
	addps	-20 * SIZE(Y), %xmm3
	movaps	%xmm3, -20 * SIZE(Y)

	addl	$16 * SIZE, X
	addl	$16 * SIZE, Y
	ALIGN_2

.L86:
	testl	$4, M
	jle	.L87

	movaps	-28 * SIZE(X), %xmm2
	movaps	-24 * SIZE(X), %xmm3

	movss	 %xmm2,  %xmm1
	shufps	 $0x93,  %xmm2, %xmm1
	PSHUFD2($0xb1,   %xmm1, %xmm5)
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm1
	movss	%xmm1,   %xmm0
	shufps	$0x93,   %xmm1, %xmm0
	addps	-32 * SIZE(Y), %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	movss	 %xmm3,  %xmm2
	shufps	 $0x93,  %xmm3, %xmm2
	PSHUFD2($0xb1,   %xmm2, %xmm5)
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm2
	movss	%xmm2,   %xmm1
	shufps	$0x93,   %xmm2, %xmm1
	addps	-28 * SIZE(Y), %xmm1
	movaps	%xmm1, -28 * SIZE(Y)

	movaps	%xmm2, %xmm0
	movaps	%xmm3, %xmm1

	addl	$8 * SIZE, X
	addl	$8 * SIZE, Y
	ALIGN_2

.L87:
	testl	$2, M
	jle	.L88

	movaps	-28 * SIZE(X), %xmm2

	movss	 %xmm2,  %xmm1
	shufps	 $0x93,  %xmm2, %xmm1
	PSHUFD2($0xb1,   %xmm1, %xmm5)
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm1
	movss	%xmm1,   %xmm0
	shufps	$0x93,   %xmm1, %xmm0
	addps	-32 * SIZE(Y), %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	movaps	%xmm1, %xmm0
	movaps	%xmm2, %xmm1

	addl	$4 * SIZE, X
	addl	$4 * SIZE, Y
	ALIGN_2

.L88:
	testl	$1, M
	jle	.L89

	movaps	-28 * SIZE(X), %xmm2

	movss	 %xmm2,  %xmm1
	shufps	 $0x93,  %xmm2, %xmm1
	PSHUFD2($0xb1,   %xmm1, %xmm5)
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm5
	addps	%xmm5,   %xmm1
	movss	%xmm1,   %xmm0
	shufps	$0x93,   %xmm1, %xmm0

	addps	-32 * SIZE(Y), %xmm0
	movlps	%xmm0, -32 * SIZE(Y)
	movhlps	%xmm0, %xmm0
	movss	%xmm0, -30 * SIZE(Y)
	jmp	.L999

.L89:
	shufps	$0x93,  %xmm0, %xmm0

	addss	-32 * SIZE(Y), %xmm0
	movss	%xmm0, -32 * SIZE(Y)
	jmp	.L999
	ALIGN_3
#endif

.L100:
	shufps	$0, ALPHA_R, ALPHA_R
	shufps	$0, ALPHA_I, ALPHA_I

#ifndef CONJ
	xorps	%xmm5, %xmm5
	subps	ALPHA_I, %xmm5

	unpcklps ALPHA_R, %xmm5
	unpcklps ALPHA_I, ALPHA_R
	movaps	 %xmm5, ALPHA_I
#else
	xorps	%xmm5, %xmm5
	subps	ALPHA_R, %xmm5

	unpcklps ALPHA_I, ALPHA_R
	unpcklps %xmm5, ALPHA_I
#endif
//If incx==0 || incy==0, avoid unloop and jump to end.
	cmpl	$0, INCX
	je  .L200
	cmpl	$0, INCY
	je	.L200

	movl	Y, YY

	movl	M,  %eax
	sarl	$3, %eax
	jle	.L105
	ALIGN_3

.L102:
	movsd	 (X), %xmm0
	addl	 INCX, X
	movhps	 (X), %xmm0
	addl	 INCX, X
	movsd	 (X), %xmm2
	addl	 INCX, X
	movhps	 (X), %xmm2
	addl	 INCX, X

#ifdef HAVE_SSE3
	movshdup	 %xmm0, %xmm1
	movsldup	 %xmm0, %xmm0
	movshdup	 %xmm2, %xmm3
	movsldup	 %xmm2, %xmm2
#else
	movaps	  %xmm0, %xmm1
	shufps	  $0xa0, %xmm0, %xmm0
	shufps	  $0xf5, %xmm1, %xmm1

	movaps	  %xmm2, %xmm3
	shufps	  $0xa0, %xmm2, %xmm2
	shufps	  $0xf5, %xmm3, %xmm3
#endif

	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm1
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm3

	movsd	(Y), %xmm4
	addl	 INCY, Y
	movhps	(Y), %xmm4
	addl	 INCY, Y
	movsd	(Y), %xmm5
	addl	 INCY, Y
	movhps	(Y), %xmm5
	addl	 INCY, Y

	addps	%xmm0, %xmm4
	addps	%xmm1, %xmm4
	addps	%xmm2, %xmm5
	addps	%xmm3, %xmm5

	movsd	%xmm4,  (YY)
	addl	 INCY, YY
	movhps	%xmm4,  (YY)
	addl	 INCY, YY
	movsd	%xmm5,  (YY)
	addl	 INCY, YY
	movhps	%xmm5,  (YY)
	addl	 INCY, YY

	movsd	 (X), %xmm0
	addl	 INCX, X
	movhps	 (X), %xmm0
	addl	 INCX, X
	movsd	 (X), %xmm2
	addl	 INCX, X
	movhps	 (X), %xmm2
	addl	 INCX, X

#ifdef HAVE_SSE3
	movshdup	 %xmm0, %xmm1
	movsldup	 %xmm0, %xmm0
	movshdup	 %xmm2, %xmm3
	movsldup	 %xmm2, %xmm2
#else
	movaps	  %xmm0, %xmm1
	shufps	  $0xa0, %xmm0, %xmm0
	shufps	  $0xf5, %xmm1, %xmm1

	movaps	  %xmm2, %xmm3
	shufps	  $0xa0, %xmm2, %xmm2
	shufps	  $0xf5, %xmm3, %xmm3
#endif

	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm1
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm3

	movsd	(Y), %xmm4
	addl	 INCY, Y
	movhps	(Y), %xmm4
	addl	 INCY, Y
	movsd	(Y), %xmm5
	addl	 INCY, Y
	movhps	(Y), %xmm5
	addl	 INCY, Y

	addps	%xmm0, %xmm4
	addps	%xmm1, %xmm4
	addps	%xmm2, %xmm5
	addps	%xmm3, %xmm5

	movsd	%xmm4,  (YY)
	addl	 INCY, YY
	movhps	%xmm4,  (YY)
	addl	 INCY, YY
	movsd	%xmm5,  (YY)
	addl	 INCY, YY
	movhps	%xmm5,  (YY)
	addl	 INCY, YY

	decl	%eax
	jg	.L102
	ALIGN_3

.L105:
	testl	$4, M
	jle	.L106

	movsd	 (X), %xmm0
	addl	 INCX, X
	movhps	 (X), %xmm0
	addl	 INCX, X
	movsd	 (X), %xmm2
	addl	 INCX, X
	movhps	 (X), %xmm2
	addl	 INCX, X

#ifdef HAVE_SSE3
	movshdup	 %xmm0, %xmm1
	movsldup	 %xmm0, %xmm0
	movshdup	 %xmm2, %xmm3
	movsldup	 %xmm2, %xmm2
#else
	movaps	  %xmm0, %xmm1
	shufps	  $0xa0, %xmm0, %xmm0
	shufps	  $0xf5, %xmm1, %xmm1

	movaps	  %xmm2, %xmm3
	shufps	  $0xa0, %xmm2, %xmm2
	shufps	  $0xf5, %xmm3, %xmm3
#endif

	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm1
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm3

	movsd	(Y), %xmm4
	addl	 INCY, Y
	movhps	(Y), %xmm4
	addl	 INCY, Y
	movsd	(Y), %xmm5
	addl	 INCY, Y
	movhps	(Y), %xmm5
	addl	 INCY, Y

	addps	%xmm0, %xmm4
	addps	%xmm1, %xmm4
	addps	%xmm2, %xmm5
	addps	%xmm3, %xmm5

	movsd	%xmm4,  (YY)
	addl	 INCY, YY
	movhps	%xmm4,  (YY)
	addl	 INCY, YY
	movsd	%xmm5,  (YY)
	addl	 INCY, YY
	movhps	%xmm5,  (YY)
	addl	 INCY, YY
	ALIGN_3

.L106:
	testl	$2, M
	jle	.L107

	movsd	 (X), %xmm0
	addl	 INCX, X
	movhps	 (X), %xmm0
	addl	 INCX, X

#ifdef HAVE_SSE3
	movshdup  %xmm0, %xmm1
	movsldup  %xmm0, %xmm0
#else
	movaps	  %xmm0, %xmm1
	shufps	  $0xa0, %xmm0, %xmm0
	shufps	  $0xf5, %xmm1, %xmm1
#endif

	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm1

	movsd	(Y), %xmm4
	addl	 INCY, Y
	movhps	(Y), %xmm4
	addl	 INCY, Y

	addps	%xmm0, %xmm4
	addps	%xmm1, %xmm4

	movsd	%xmm4,  (YY)
	addl	 INCY, YY
	movhps	%xmm4,  (YY)
	addl	 INCY, YY
	ALIGN_3

.L107:
	testl	$1, M
	jle	.L999

	movsd	 (X), %xmm0

#ifdef HAVE_SSE3
	movshdup  %xmm0, %xmm1
	movsldup  %xmm0, %xmm0
#else
	movaps	  %xmm0, %xmm1
	shufps	  $0xa0, %xmm0, %xmm0
	shufps	  $0xf5, %xmm1, %xmm1
#endif

	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm1

	movsd	 (Y), %xmm4

	addps	%xmm0, %xmm4
	addps	%xmm1, %xmm4

	movsd	%xmm4,   (Y)
	jmp	.L999
	ALIGN_3

.L200:
	movl    M, %eax
	cmpl	$0, %eax
	jle .L999
	ALIGN_3

.L201:
	movsd	 (X), %xmm0

#ifdef HAVE_SSE3
	movshdup  %xmm0, %xmm1
	movsldup  %xmm0, %xmm0
#else
	movaps	  %xmm0, %xmm1
	shufps	  $0xa0, %xmm0, %xmm0
	shufps	  $0xf5, %xmm1, %xmm1
#endif

	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm1

	movsd	 (Y), %xmm4

	addps	%xmm0, %xmm4
	addps	%xmm1, %xmm4

	movsd	%xmm4,   (Y)

	decl    %eax
	jg  .L201

	ALIGN_3
.L999:
	popl	%ebp
	popl	%ebx
	popl	%esi
	popl	%edi
	ret

	EPILOGUE

