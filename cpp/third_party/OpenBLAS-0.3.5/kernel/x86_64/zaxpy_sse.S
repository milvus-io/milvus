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

#ifndef WINDOWS_ABI
#define M	ARG1
#define X	ARG4
#define INCX	ARG5
#define Y	ARG6
#define INCY	ARG2
#else
#define M	ARG1
#define X	ARG2
#define INCX	ARG3
#define Y	ARG4
#define INCY	%r10
#endif

#define	YY	%r11

#define ALPHA_R	%xmm14
#define ALPHA_I	%xmm15

#include "l1param.h"

	PROLOGUE
	PROFCODE

#ifndef WINDOWS_ABI
	movq	 8(%rsp), INCY
#else
	movaps	%xmm3, %xmm0
	movss	40(%rsp), %xmm1

	movq	48(%rsp), X
	movq	56(%rsp), INCX
	movq	64(%rsp), Y
	movq	72(%rsp), INCY
#endif

	SAVEREGISTERS

	salq	$ZBASE_SHIFT, INCX
	salq	$ZBASE_SHIFT, INCY

	testq	M, M
	jle	.L999

	cmpq	$2 * SIZE, INCX
	jne	.L100
	cmpq	$2 * SIZE, INCY
	jne	.L100

	pcmpeqb	%xmm7, %xmm7
	psllq	$63,   %xmm7

	pshufd	$0, %xmm0, ALPHA_R
	pshufd	$0, %xmm1, ALPHA_I

#ifndef CONJ
	shufps	$0xb1, %xmm7, %xmm7
	xorpd	%xmm7, ALPHA_I
#else
	xorpd	%xmm7, ALPHA_R
#endif

	subq	$-32 * SIZE, X
	subq	$-32 * SIZE, Y

	testq	$2 * SIZE, Y
	je	.L10

	movsd	-32 * SIZE(X), %xmm0
	movsd	-32 * SIZE(Y), %xmm1

	pshufd	$0xb1,   %xmm0, %xmm8
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm8

	addps	%xmm8,   %xmm0
	addps	%xmm1,   %xmm0

	movlps	%xmm0, -32 * SIZE(Y)

	addq	$2 * SIZE, X
	addq	$2 * SIZE, Y
	decq	M
	jle	.L999
	ALIGN_2

.L10:
	testq	$SIZE, Y
	jne	.L50

	testq	$3 * SIZE, X
	jne	.L20

	movq	M,  %rax
	sarq	$4, %rax
	jle	.L15

	movaps	-32 * SIZE(X), %xmm0
	movaps	-28 * SIZE(X), %xmm1
	movaps	-24 * SIZE(X), %xmm2
	movaps	-20 * SIZE(X), %xmm3

	decq	%rax
	jle	.L12
	ALIGN_3

.L11:
	movaps	-16 * SIZE(X), %xmm4
	movaps	-12 * SIZE(X), %xmm5

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	pshufd	$0xb1,   %xmm0, %xmm8
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm8
	addps	-32 * SIZE(Y), %xmm0
	addps	%xmm8,   %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	pshufd	$0xb1,   %xmm1, %xmm8
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm8
	addps	-28 * SIZE(Y), %xmm1
	addps	%xmm8,   %xmm1
	movaps	%xmm1, -28 * SIZE(Y)

	movaps	 -8 * SIZE(X), %xmm6
	movaps	 -4 * SIZE(X), %xmm7

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	pshufd	$0xb1,   %xmm2, %xmm8
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm8
	addps	-24 * SIZE(Y), %xmm2
	addps	%xmm8,   %xmm2
	movaps	%xmm2, -24 * SIZE(Y)

	pshufd	$0xb1,   %xmm3, %xmm8
	mulps	ALPHA_R, %xmm3
	mulps	ALPHA_I, %xmm8
	addps	-20 * SIZE(Y), %xmm3
	addps	%xmm8,   %xmm3
	movaps	%xmm3, -20 * SIZE(Y)

	movaps	  0 * SIZE(X), %xmm0
	movaps	  4 * SIZE(X), %xmm1

#if defined(PREFETCHW) && !defined(FETCH128)
	PREFETCHW (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	pshufd	$0xb1,   %xmm4, %xmm8
	mulps	ALPHA_R, %xmm4
	mulps	ALPHA_I, %xmm8
	addps	-16 * SIZE(Y), %xmm4
	addps	%xmm8,   %xmm4
	movaps	%xmm4, -16 * SIZE(Y)

	pshufd	$0xb1,   %xmm5, %xmm8
	mulps	ALPHA_R, %xmm5
	mulps	ALPHA_I, %xmm8
	addps	-12 * SIZE(Y), %xmm5
	addps	%xmm8,   %xmm5
	movaps	%xmm5, -12 * SIZE(Y)

	movaps	  8 * SIZE(X), %xmm2
	movaps	 12 * SIZE(X), %xmm3

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	pshufd	$0xb1,   %xmm6, %xmm8
	mulps	ALPHA_R, %xmm6
	mulps	ALPHA_I, %xmm8
	addps	 -8 * SIZE(Y), %xmm6
	addps	%xmm8,   %xmm6
	movaps	%xmm6,  -8 * SIZE(Y)

	pshufd	$0xb1,   %xmm7, %xmm8
	mulps	ALPHA_R, %xmm7
	mulps	ALPHA_I, %xmm8
	addps	 -4 * SIZE(Y), %xmm7
	addps	%xmm8,   %xmm7
	movaps	%xmm7,  -4 * SIZE(Y)

	subq	$-32 * SIZE, X
	subq	$-32 * SIZE, Y
	decq	%rax
	jg	.L11
	ALIGN_3

.L12:
	movaps	-16 * SIZE(X), %xmm4
	movaps	-12 * SIZE(X), %xmm5

	pshufd	$0xb1,   %xmm0, %xmm8
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm8
	addps	-32 * SIZE(Y), %xmm0
	addps	%xmm8,   %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	pshufd	$0xb1,   %xmm1, %xmm8
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm8
	addps	-28 * SIZE(Y), %xmm1
	addps	%xmm8,   %xmm1
	movaps	%xmm1, -28 * SIZE(Y)

	movaps	 -8 * SIZE(X), %xmm6
	movaps	 -4 * SIZE(X), %xmm7

	pshufd	$0xb1,   %xmm2, %xmm8
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm8
	addps	-24 * SIZE(Y), %xmm2
	addps	%xmm8,   %xmm2
	movaps	%xmm2, -24 * SIZE(Y)

	pshufd	$0xb1,   %xmm3, %xmm8
	mulps	ALPHA_R, %xmm3
	mulps	ALPHA_I, %xmm8
	addps	-20 * SIZE(Y), %xmm3
	addps	%xmm8,   %xmm3
	movaps	%xmm3, -20 * SIZE(Y)

	pshufd	$0xb1,   %xmm4, %xmm8
	mulps	ALPHA_R, %xmm4
	mulps	ALPHA_I, %xmm8
	addps	-16 * SIZE(Y), %xmm4
	addps	%xmm8,   %xmm4
	movaps	%xmm4, -16 * SIZE(Y)

	pshufd	$0xb1,   %xmm5, %xmm8
	mulps	ALPHA_R, %xmm5
	mulps	ALPHA_I, %xmm8
	addps	-12 * SIZE(Y), %xmm5
	addps	%xmm8,   %xmm5
	movaps	%xmm5, -12 * SIZE(Y)

	pshufd	$0xb1,   %xmm6, %xmm8
	mulps	ALPHA_R, %xmm6
	mulps	ALPHA_I, %xmm8
	addps	 -8 * SIZE(Y), %xmm6
	addps	%xmm8,   %xmm6
	movaps	%xmm6,  -8 * SIZE(Y)

	pshufd	$0xb1,   %xmm7, %xmm8
	mulps	ALPHA_R, %xmm7
	mulps	ALPHA_I, %xmm8
	addps	 -4 * SIZE(Y), %xmm7
	addps	%xmm8,   %xmm7
	movaps	%xmm7,  -4 * SIZE(Y)

	subq	$-32 * SIZE, X
	subq	$-32 * SIZE, Y
	ALIGN_3

.L15:
	testq	$8, M
	jle	.L16

	movaps	-32 * SIZE(X), %xmm0
	movaps	-28 * SIZE(X), %xmm1

	pshufd	$0xb1,   %xmm0, %xmm8
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm8
	addps	-32 * SIZE(Y), %xmm0
	addps	%xmm8,   %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	pshufd	$0xb1,   %xmm1, %xmm8
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm8
	addps	-28 * SIZE(Y), %xmm1
	addps	%xmm8,   %xmm1
	movaps	%xmm1, -28 * SIZE(Y)

	movaps	-24 * SIZE(X), %xmm2
	movaps	-20 * SIZE(X), %xmm3

	pshufd	$0xb1,   %xmm2, %xmm8
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm8
	addps	-24 * SIZE(Y), %xmm2
	addps	%xmm8,   %xmm2
	movaps	%xmm2, -24 * SIZE(Y)

	pshufd	$0xb1,   %xmm3, %xmm8
	mulps	ALPHA_R, %xmm3
	mulps	ALPHA_I, %xmm8
	addps	-20 * SIZE(Y), %xmm3
	addps	%xmm8,   %xmm3
	movaps	%xmm3, -20 * SIZE(Y)

	addq	$16 * SIZE, X
	addq	$16 * SIZE, Y
	ALIGN_2

.L16:
	testq	$4, M
	jle	.L17

	movaps	-32 * SIZE(X), %xmm0
	movaps	-28 * SIZE(X), %xmm1

	pshufd	$0xb1,   %xmm0, %xmm8
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm8
	addps	-32 * SIZE(Y), %xmm0
	addps	%xmm8,   %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	pshufd	$0xb1,   %xmm1, %xmm8
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm8
	addps	-28 * SIZE(Y), %xmm1
	addps	%xmm8,   %xmm1
	movaps	%xmm1, -28 * SIZE(Y)

	addq	$8 * SIZE, X
	addq	$8 * SIZE, Y
	ALIGN_2

.L17:
	testq	$2, M
	jle	.L18

	movaps	-32 * SIZE(X), %xmm0

	pshufd	$0xb1,   %xmm0, %xmm8
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm8

	addps	-32 * SIZE(Y), %xmm0
	addps	%xmm8,   %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	addq	$4 * SIZE, X
	addq	$4 * SIZE, Y
	ALIGN_2

.L18:
	testq	$1, M
	jle	.L999

	movsd	-32 * SIZE(X), %xmm0

	pshufd	$0xb1,   %xmm0, %xmm8
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm8

	movsd	-32 * SIZE(Y), %xmm1
	addps	%xmm1,   %xmm0
	addps	%xmm8,   %xmm0
	movlps	%xmm0, -32 * SIZE(Y)
	jmp	.L999
	ALIGN_3

.L20:
#ifdef ALIGNED_ACCESS

	testq	$2 * SIZE, X
	jne	.L30

	subq	$1 * SIZE, X

	movaps	-32 * SIZE(X), %xmm0

	movq	M,  %rax
	sarq	$4, %rax
	jle	.L25

	movaps	-28 * SIZE(X), %xmm1
	movaps	-24 * SIZE(X), %xmm2
	movaps	-20 * SIZE(X), %xmm3
	movaps	-16 * SIZE(X), %xmm4

	decq	%rax
	jle	.L22
	ALIGN_3

.L21:
	movaps	-12 * SIZE(X), %xmm5
	movaps	 -8 * SIZE(X), %xmm6

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	movss	 %xmm1,  %xmm0
	SHUFPS_39  %xmm0, %xmm0
	pshufd	$0xb1,   %xmm0, %xmm8
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm8
	addps	-32 * SIZE(Y), %xmm0
	addps	%xmm8,   %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	movss	 %xmm2,  %xmm1
	SHUFPS_39  %xmm1, %xmm1
	pshufd	$0xb1,   %xmm1, %xmm8
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm8
	addps	-28 * SIZE(Y), %xmm1
	addps	%xmm8,   %xmm1
	movaps	%xmm1, -28 * SIZE(Y)

	movaps	 -4 * SIZE(X), %xmm7
	movaps	  0 * SIZE(X), %xmm0

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movss	 %xmm3,  %xmm2
	SHUFPS_39  %xmm2, %xmm2
	pshufd	$0xb1,   %xmm2, %xmm8
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm8
	addps	-24 * SIZE(Y), %xmm2
	addps	%xmm8,   %xmm2
	movaps	%xmm2, -24 * SIZE(Y)

	movss	 %xmm4,  %xmm3
	SHUFPS_39  %xmm3, %xmm3
	pshufd	$0xb1,   %xmm3, %xmm8
	mulps	ALPHA_R, %xmm3
	mulps	ALPHA_I, %xmm8
	addps	-20 * SIZE(Y), %xmm3
	addps	%xmm8,   %xmm3
	movaps	%xmm3, -20 * SIZE(Y)

	movaps	  4 * SIZE(X), %xmm1
	movaps	  8 * SIZE(X), %xmm2

#if defined(PREFETCHW) && !defined(FETCH128)
	PREFETCHW (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	movss	 %xmm5,  %xmm4
	SHUFPS_39  %xmm4, %xmm4
	pshufd	$0xb1,   %xmm4, %xmm8
	mulps	ALPHA_R, %xmm4
	mulps	ALPHA_I, %xmm8
	addps	-16 * SIZE(Y), %xmm4
	addps	%xmm8,   %xmm4
	movaps	%xmm4, -16 * SIZE(Y)

	movss	 %xmm6,  %xmm5
	SHUFPS_39  %xmm5, %xmm5
	pshufd	$0xb1,   %xmm5, %xmm8
	mulps	ALPHA_R, %xmm5
	mulps	ALPHA_I, %xmm8
	addps	-12 * SIZE(Y), %xmm5
	addps	%xmm8,   %xmm5
	movaps	%xmm5, -12 * SIZE(Y)

	movaps	 12 * SIZE(X), %xmm3
	movaps	 16 * SIZE(X), %xmm4

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	movss	 %xmm7,  %xmm6
	SHUFPS_39  %xmm6, %xmm6
	pshufd	$0xb1,   %xmm6, %xmm8
	mulps	ALPHA_R, %xmm6
	mulps	ALPHA_I, %xmm8
	addps	 -8 * SIZE(Y), %xmm6
	addps	%xmm8,   %xmm6
	movaps	%xmm6,  -8 * SIZE(Y)

	movss	 %xmm0,  %xmm7
	SHUFPS_39  %xmm7, %xmm7
	pshufd	$0xb1,   %xmm7, %xmm8
	mulps	ALPHA_R, %xmm7
	mulps	ALPHA_I, %xmm8
	addps	 -4 * SIZE(Y), %xmm7
	addps	%xmm8,   %xmm7
	movaps	%xmm7,  -4 * SIZE(Y)

	subq	$-32 * SIZE, X
	subq	$-32 * SIZE, Y
	decq	%rax
	jg	.L21
	ALIGN_3

.L22:
	movaps	-12 * SIZE(X), %xmm5
	movaps	 -8 * SIZE(X), %xmm6

	movss	 %xmm1,  %xmm0
	SHUFPS_39  %xmm0, %xmm0
	pshufd	$0xb1,   %xmm0, %xmm8
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm8
	addps	-32 * SIZE(Y), %xmm0
	addps	%xmm8,   %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	movss	 %xmm2,  %xmm1
	SHUFPS_39  %xmm1, %xmm1
	pshufd	$0xb1,   %xmm1, %xmm8
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm8
	addps	-28 * SIZE(Y), %xmm1
	addps	%xmm8,   %xmm1
	movaps	%xmm1, -28 * SIZE(Y)

	movaps	 -4 * SIZE(X), %xmm7
	movaps	  0 * SIZE(X), %xmm0

	movss	 %xmm3,  %xmm2
	SHUFPS_39  %xmm2, %xmm2
	pshufd	$0xb1,   %xmm2, %xmm8
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm8
	addps	-24 * SIZE(Y), %xmm2
	addps	%xmm8,   %xmm2
	movaps	%xmm2, -24 * SIZE(Y)

	movss	 %xmm4,  %xmm3
	SHUFPS_39  %xmm3, %xmm3
	pshufd	$0xb1,   %xmm3, %xmm8
	mulps	ALPHA_R, %xmm3
	mulps	ALPHA_I, %xmm8
	addps	-20 * SIZE(Y), %xmm3
	addps	%xmm8,   %xmm3
	movaps	%xmm3, -20 * SIZE(Y)

	movss	 %xmm5,  %xmm4
	SHUFPS_39  %xmm4, %xmm4
	pshufd	$0xb1,   %xmm4, %xmm8
	mulps	ALPHA_R, %xmm4
	mulps	ALPHA_I, %xmm8
	addps	-16 * SIZE(Y), %xmm4
	addps	%xmm8,   %xmm4
	movaps	%xmm4, -16 * SIZE(Y)

	movss	 %xmm6,  %xmm5
	SHUFPS_39  %xmm5, %xmm5
	pshufd	$0xb1,   %xmm5, %xmm8
	mulps	ALPHA_R, %xmm5
	mulps	ALPHA_I, %xmm8
	addps	-12 * SIZE(Y), %xmm5
	addps	%xmm8,   %xmm5
	movaps	%xmm5, -12 * SIZE(Y)

	movss	 %xmm7,  %xmm6
	SHUFPS_39  %xmm6, %xmm6
	pshufd	$0xb1,   %xmm6, %xmm8
	mulps	ALPHA_R, %xmm6
	mulps	ALPHA_I, %xmm8
	addps	 -8 * SIZE(Y), %xmm6
	addps	%xmm8,   %xmm6
	movaps	%xmm6,  -8 * SIZE(Y)

	movss	 %xmm0,  %xmm7
	SHUFPS_39  %xmm7, %xmm7
	pshufd	$0xb1,   %xmm7, %xmm8
	mulps	ALPHA_R, %xmm7
	mulps	ALPHA_I, %xmm8
	addps	 -4 * SIZE(Y), %xmm7
	addps	%xmm8,   %xmm7
	movaps	%xmm7,  -4 * SIZE(Y)

	subq	$-32 * SIZE, X
	subq	$-32 * SIZE, Y
	ALIGN_3

.L25:
	testq	$8, M
	jle	.L26

	movaps	-28 * SIZE(X), %xmm1
	movaps	-24 * SIZE(X), %xmm2

	movss	 %xmm1,  %xmm0
	SHUFPS_39  %xmm0, %xmm0
	pshufd	$0xb1,   %xmm0, %xmm8
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm8
	addps	-32 * SIZE(Y), %xmm0
	addps	%xmm8,   %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	movss	 %xmm2,  %xmm1
	SHUFPS_39  %xmm1, %xmm1
	pshufd	$0xb1,   %xmm1, %xmm8
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm8
	addps	-28 * SIZE(Y), %xmm1
	addps	%xmm8,   %xmm1
	movaps	%xmm1, -28 * SIZE(Y)

	movaps	-20 * SIZE(X), %xmm3
	movaps	-16 * SIZE(X), %xmm0

	movss	 %xmm3,  %xmm2
	SHUFPS_39  %xmm2, %xmm2
	pshufd	$0xb1,   %xmm2, %xmm8
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm8
	addps	-24 * SIZE(Y), %xmm2
	addps	%xmm8,   %xmm2
	movaps	%xmm2, -24 * SIZE(Y)

	movss	 %xmm0,  %xmm3
	SHUFPS_39  %xmm3, %xmm3
	pshufd	$0xb1,   %xmm3, %xmm8
	mulps	ALPHA_R, %xmm3
	mulps	ALPHA_I, %xmm8
	addps	-20 * SIZE(Y), %xmm3
	addps	%xmm8,   %xmm3
	movaps	%xmm3, -20 * SIZE(Y)

	addq	$16 * SIZE, X
	addq	$16 * SIZE, Y
	ALIGN_2

.L26:
	testq	$4, M
	jle	.L27

	movaps	-28 * SIZE(X), %xmm1
	movaps	-24 * SIZE(X), %xmm2

	movss	 %xmm1,  %xmm0
	SHUFPS_39  %xmm0, %xmm0
	pshufd	$0xb1,   %xmm0, %xmm8
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm8
	addps	-32 * SIZE(Y), %xmm0
	addps	%xmm8,   %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	movss	 %xmm2,  %xmm1
	SHUFPS_39  %xmm1, %xmm1
	pshufd	$0xb1,   %xmm1, %xmm8
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm8
	addps	-28 * SIZE(Y), %xmm1
	addps	%xmm8,   %xmm1
	movaps	%xmm1, -28 * SIZE(Y)

	movaps	%xmm2, %xmm0

	addq	$8 * SIZE, X
	addq	$8 * SIZE, Y
	ALIGN_2

.L27:
	testq	$2, M
	jle	.L28

	movaps	-28 * SIZE(X), %xmm1

	movss	 %xmm1,  %xmm0
	SHUFPS_39  %xmm0, %xmm0
	pshufd	$0xb1,   %xmm0, %xmm8
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm8

	addps	-32 * SIZE(Y), %xmm0
	addps	%xmm8,   %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	movaps	%xmm1, %xmm0

	addq	$4 * SIZE, X
	addq	$4 * SIZE, Y
	ALIGN_2

.L28:
	testq	$1, M
	jle	.L999

	pshufd	$0x06, %xmm0, %xmm8
	pshufd	$0x09, %xmm0, %xmm0

	mulps	ALPHA_I, %xmm8
	mulps	ALPHA_R, %xmm0

	addps	-32 * SIZE(Y), %xmm8
	addps	%xmm8,   %xmm0

	movlps	%xmm0, -32 * SIZE(Y)

	jmp	.L999
	ALIGN_3

.L30:
	testq	$1 * SIZE, X
	jne	.L40
#endif

	movq	M,  %rax
	sarq	$4, %rax
	jle	.L35

	movsd	-32 * SIZE(X), %xmm0
	movhps	-30 * SIZE(X), %xmm0
	movsd	-28 * SIZE(X), %xmm1
	movhps	-26 * SIZE(X), %xmm1
	movsd	-24 * SIZE(X), %xmm2
	movhps	-22 * SIZE(X), %xmm2
	movsd	-20 * SIZE(X), %xmm3
	movhps	-18 * SIZE(X), %xmm3

	decq	%rax
	jle	.L32
	ALIGN_3

.L31:
	movsd	-16 * SIZE(X), %xmm4
	movhps	-14 * SIZE(X), %xmm4

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	pshufd	$0xb1,   %xmm0, %xmm8
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm8
	addps	-32 * SIZE(Y), %xmm0
	addps	%xmm8,   %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	movsd	-12 * SIZE(X), %xmm5
	movhps	-10 * SIZE(X), %xmm5

	pshufd	$0xb1,   %xmm1, %xmm8
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm8
	addps	-28 * SIZE(Y), %xmm1
	addps	%xmm8,   %xmm1
	movaps	%xmm1, -28 * SIZE(Y)

	movsd	 -8 * SIZE(X), %xmm6
	movhps	 -6 * SIZE(X), %xmm6

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	pshufd	$0xb1,   %xmm2, %xmm8
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm8
	addps	-24 * SIZE(Y), %xmm2
	addps	%xmm8,   %xmm2
	movaps	%xmm2, -24 * SIZE(Y)

	movsd	 -4 * SIZE(X), %xmm7
	movhps	 -2 * SIZE(X), %xmm7

	pshufd	$0xb1,   %xmm3, %xmm8
	mulps	ALPHA_R, %xmm3
	mulps	ALPHA_I, %xmm8
	addps	-20 * SIZE(Y), %xmm3
	addps	%xmm8,   %xmm3
	movaps	%xmm3, -20 * SIZE(Y)

	movsd	  0 * SIZE(X), %xmm0
	movhps	  2 * SIZE(X), %xmm0

#if defined(PREFETCHW) && !defined(FETCH128)
	PREFETCHW (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	pshufd	$0xb1,   %xmm4, %xmm8
	mulps	ALPHA_R, %xmm4
	mulps	ALPHA_I, %xmm8
	addps	-16 * SIZE(Y), %xmm4
	addps	%xmm8,   %xmm4
	movaps	%xmm4, -16 * SIZE(Y)

	movsd	  4 * SIZE(X), %xmm1
	movhps	  6 * SIZE(X), %xmm1

	pshufd	$0xb1,   %xmm5, %xmm8
	mulps	ALPHA_R, %xmm5
	mulps	ALPHA_I, %xmm8
	addps	-12 * SIZE(Y), %xmm5
	addps	%xmm8,   %xmm5
	movaps	%xmm5, -12 * SIZE(Y)

	movsd	  8 * SIZE(X), %xmm2
	movhps	 10 * SIZE(X), %xmm2

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	pshufd	$0xb1,   %xmm6, %xmm8
	mulps	ALPHA_R, %xmm6
	mulps	ALPHA_I, %xmm8
	addps	 -8 * SIZE(Y), %xmm6
	addps	%xmm8,   %xmm6
	movaps	%xmm6,  -8 * SIZE(Y)

	movsd	 12 * SIZE(X), %xmm3
	movhps	 14 * SIZE(X), %xmm3

	pshufd	$0xb1,   %xmm7, %xmm8
	mulps	ALPHA_R, %xmm7
	mulps	ALPHA_I, %xmm8
	addps	 -4 * SIZE(Y), %xmm7
	addps	%xmm8,   %xmm7
	movaps	%xmm7,  -4 * SIZE(Y)

	subq	$-32 * SIZE, X
	subq	$-32 * SIZE, Y
	decq	%rax
	jg	.L31
	ALIGN_3

.L32:
	movsd	-16 * SIZE(X), %xmm4
	movhps	-14 * SIZE(X), %xmm4

	pshufd	$0xb1,   %xmm0, %xmm8
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm8
	addps	-32 * SIZE(Y), %xmm0
	addps	%xmm8,   %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	movsd	-12 * SIZE(X), %xmm5
	movhps	-10 * SIZE(X), %xmm5

	pshufd	$0xb1,   %xmm1, %xmm8
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm8
	addps	-28 * SIZE(Y), %xmm1
	addps	%xmm8,   %xmm1
	movaps	%xmm1, -28 * SIZE(Y)

	movsd	 -8 * SIZE(X), %xmm6
	movhps	 -6 * SIZE(X), %xmm6

	pshufd	$0xb1,   %xmm2, %xmm8
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm8
	addps	-24 * SIZE(Y), %xmm2
	addps	%xmm8,   %xmm2
	movaps	%xmm2, -24 * SIZE(Y)

	movsd	 -4 * SIZE(X), %xmm7
	movhps	 -2 * SIZE(X), %xmm7

	pshufd	$0xb1,   %xmm3, %xmm8
	mulps	ALPHA_R, %xmm3
	mulps	ALPHA_I, %xmm8
	addps	-20 * SIZE(Y), %xmm3
	addps	%xmm8,   %xmm3
	movaps	%xmm3, -20 * SIZE(Y)

	pshufd	$0xb1,   %xmm4, %xmm8
	mulps	ALPHA_R, %xmm4
	mulps	ALPHA_I, %xmm8
	addps	-16 * SIZE(Y), %xmm4
	addps	%xmm8,   %xmm4
	movaps	%xmm4, -16 * SIZE(Y)

	pshufd	$0xb1,   %xmm5, %xmm8
	mulps	ALPHA_R, %xmm5
	mulps	ALPHA_I, %xmm8
	addps	-12 * SIZE(Y), %xmm5
	addps	%xmm8,   %xmm5
	movaps	%xmm5, -12 * SIZE(Y)

	pshufd	$0xb1,   %xmm6, %xmm8
	mulps	ALPHA_R, %xmm6
	mulps	ALPHA_I, %xmm8
	addps	 -8 * SIZE(Y), %xmm6
	addps	%xmm8,   %xmm6
	movaps	%xmm6,  -8 * SIZE(Y)

	pshufd	$0xb1,   %xmm7, %xmm8
	mulps	ALPHA_R, %xmm7
	mulps	ALPHA_I, %xmm8
	addps	 -4 * SIZE(Y), %xmm7
	addps	%xmm8,   %xmm7
	movaps	%xmm7,  -4 * SIZE(Y)

	subq	$-32 * SIZE, X
	subq	$-32 * SIZE, Y
	ALIGN_3

.L35:
	testq	$8, M
	jle	.L36

	movsd	-32 * SIZE(X), %xmm0
	movhps	-30 * SIZE(X), %xmm0
	movsd	-28 * SIZE(X), %xmm1
	movhps	-26 * SIZE(X), %xmm1

	pshufd	$0xb1,   %xmm0, %xmm8
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm8
	addps	-32 * SIZE(Y), %xmm0
	addps	%xmm8,   %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	pshufd	$0xb1,   %xmm1, %xmm8
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm8
	addps	-28 * SIZE(Y), %xmm1
	addps	%xmm8,   %xmm1
	movaps	%xmm1, -28 * SIZE(Y)

	movsd	-24 * SIZE(X), %xmm2
	movhps	-22 * SIZE(X), %xmm2
	movsd	-20 * SIZE(X), %xmm3
	movhps	-18 * SIZE(X), %xmm3

	pshufd	$0xb1,   %xmm2, %xmm8
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm8
	addps	-24 * SIZE(Y), %xmm2
	addps	%xmm8,   %xmm2
	movaps	%xmm2, -24 * SIZE(Y)

	pshufd	$0xb1,   %xmm3, %xmm8
	mulps	ALPHA_R, %xmm3
	mulps	ALPHA_I, %xmm8
	addps	-20 * SIZE(Y), %xmm3
	addps	%xmm8,   %xmm3
	movaps	%xmm3, -20 * SIZE(Y)

	addq	$16 * SIZE, X
	addq	$16 * SIZE, Y
	ALIGN_2

.L36:
	testq	$4, M
	jle	.L37

	movsd	-32 * SIZE(X), %xmm0
	movhps	-30 * SIZE(X), %xmm0
	movsd	-28 * SIZE(X), %xmm1
	movhps	-26 * SIZE(X), %xmm1

	pshufd	$0xb1,   %xmm0, %xmm8
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm8
	addps	-32 * SIZE(Y), %xmm0
	addps	%xmm8,   %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	pshufd	$0xb1,   %xmm1, %xmm8
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm8
	addps	-28 * SIZE(Y), %xmm1
	addps	%xmm8,   %xmm1
	movaps	%xmm1, -28 * SIZE(Y)

	addq	$8 * SIZE, X
	addq	$8 * SIZE, Y
	ALIGN_2

.L37:
	testq	$2, M
	jle	.L38

	movsd	-32 * SIZE(X), %xmm0
	movhps	-30 * SIZE(X), %xmm0

	pshufd	$0xb1,   %xmm0, %xmm8
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm8

	addps	-32 * SIZE(Y), %xmm0
	addps	%xmm8,   %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	addq	$4 * SIZE, X
	addq	$4 * SIZE, Y
	ALIGN_2

.L38:
	testq	$1, M
	jle	.L999

	movsd	-32 * SIZE(X), %xmm0

	pshufd	$0xb1,   %xmm0, %xmm8
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm8

	movsd	-32 * SIZE(Y), %xmm1

	addps	%xmm1, %xmm0
	addps	%xmm8,   %xmm0
	movlps	%xmm0, -32 * SIZE(Y)
	jmp	.L999
	ALIGN_3

#ifdef ALIGNED_ACCESS

.L40:
	subq	$3 * SIZE, X

	movaps	-32 * SIZE(X), %xmm0

	movq	M,  %rax
	sarq	$4, %rax
	jle	.L45

	movaps	-28 * SIZE(X), %xmm1
	movaps	-24 * SIZE(X), %xmm2
	movaps	-20 * SIZE(X), %xmm3
	movaps	-16 * SIZE(X), %xmm4

	decq	%rax
	jle	.L42
	ALIGN_3

.L41:
	movaps	-12 * SIZE(X), %xmm5
	movaps	 -8 * SIZE(X), %xmm6

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	movss	 %xmm1,  %xmm0
	shufps	 $0x93,  %xmm1, %xmm0
	pshufd	$0xb1,   %xmm0, %xmm8
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm8
	addps	-32 * SIZE(Y), %xmm0
	addps	%xmm8,   %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	movss	 %xmm2,  %xmm1
	shufps	 $0x93,  %xmm2, %xmm1
	pshufd	$0xb1,   %xmm1, %xmm8
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm8
	addps	-28 * SIZE(Y), %xmm1
	addps	%xmm8,   %xmm1
	movaps	%xmm1, -28 * SIZE(Y)

	movaps	 -4 * SIZE(X), %xmm7
	movaps	  0 * SIZE(X), %xmm0

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movss	 %xmm3,  %xmm2
	shufps	 $0x93,  %xmm3, %xmm2
	pshufd	$0xb1,   %xmm2, %xmm8
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm8
	addps	-24 * SIZE(Y), %xmm2
	addps	%xmm8,   %xmm2
	movaps	%xmm2, -24 * SIZE(Y)

	movss	 %xmm4,  %xmm3
	shufps	 $0x93,  %xmm4, %xmm3
	pshufd	$0xb1,   %xmm3, %xmm8
	mulps	ALPHA_R, %xmm3
	mulps	ALPHA_I, %xmm8
	addps	-20 * SIZE(Y), %xmm3
	addps	%xmm8,   %xmm3
	movaps	%xmm3, -20 * SIZE(Y)

	movaps	  4 * SIZE(X), %xmm1
	movaps	  8 * SIZE(X), %xmm2

#if defined(PREFETCHW) && !defined(FETCH128)
	PREFETCHW (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	movss	 %xmm5,  %xmm4
	shufps	 $0x93,  %xmm5, %xmm4
	pshufd	$0xb1,   %xmm4, %xmm8
	mulps	ALPHA_R, %xmm4
	mulps	ALPHA_I, %xmm8
	addps	-16 * SIZE(Y), %xmm4
	addps	%xmm8,   %xmm4
	movaps	%xmm4, -16 * SIZE(Y)

	movss	 %xmm6,  %xmm5
	shufps	 $0x93,  %xmm6, %xmm5
	pshufd	$0xb1,   %xmm5, %xmm8
	mulps	ALPHA_R, %xmm5
	mulps	ALPHA_I, %xmm8
	addps	-12 * SIZE(Y), %xmm5
	addps	%xmm8,   %xmm5
	movaps	%xmm5, -12 * SIZE(Y)

	movaps	 12 * SIZE(X), %xmm3
	movaps	 16 * SIZE(X), %xmm4

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	movss	 %xmm7,  %xmm6
	shufps	 $0x93,  %xmm7, %xmm6
	pshufd	$0xb1,   %xmm6, %xmm8
	mulps	ALPHA_R, %xmm6
	mulps	ALPHA_I, %xmm8
	addps	 -8 * SIZE(Y), %xmm6
	addps	%xmm8,   %xmm6
	movaps	%xmm6,  -8 * SIZE(Y)

	movss	 %xmm0,  %xmm7
	shufps	 $0x93,  %xmm0, %xmm7
	pshufd	$0xb1,   %xmm7, %xmm8
	mulps	ALPHA_R, %xmm7
	mulps	ALPHA_I, %xmm8
	addps	 -4 * SIZE(Y), %xmm7
	addps	%xmm8,   %xmm7
	movaps	%xmm7,  -4 * SIZE(Y)

	subq	$-32 * SIZE, X
	subq	$-32 * SIZE, Y
	decq	%rax
	jg	.L41
	ALIGN_3

.L42:
	movaps	-12 * SIZE(X), %xmm5
	movaps	 -8 * SIZE(X), %xmm6

	movss	 %xmm1,  %xmm0
	shufps	 $0x93,  %xmm1, %xmm0
	pshufd	$0xb1,   %xmm0, %xmm8
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm8
	addps	-32 * SIZE(Y), %xmm0
	addps	%xmm8,   %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	movss	 %xmm2,  %xmm1
	shufps	 $0x93,  %xmm2, %xmm1
	pshufd	$0xb1,   %xmm1, %xmm8
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm8
	addps	-28 * SIZE(Y), %xmm1
	addps	%xmm8,   %xmm1
	movaps	%xmm1, -28 * SIZE(Y)

	movaps	 -4 * SIZE(X), %xmm7
	movaps	  0 * SIZE(X), %xmm0

	movss	 %xmm3,  %xmm2
	shufps	 $0x93,  %xmm3, %xmm2
	pshufd	$0xb1,   %xmm2, %xmm8
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm8
	addps	-24 * SIZE(Y), %xmm2
	addps	%xmm8,   %xmm2
	movaps	%xmm2, -24 * SIZE(Y)

	movss	 %xmm4,  %xmm3
	shufps	 $0x93,  %xmm4, %xmm3
	pshufd	$0xb1,   %xmm3, %xmm8
	mulps	ALPHA_R, %xmm3
	mulps	ALPHA_I, %xmm8
	addps	-20 * SIZE(Y), %xmm3
	addps	%xmm8,   %xmm3
	movaps	%xmm3, -20 * SIZE(Y)

	movss	 %xmm5,  %xmm4
	shufps	 $0x93,  %xmm5, %xmm4
	pshufd	$0xb1,   %xmm4, %xmm8
	mulps	ALPHA_R, %xmm4
	mulps	ALPHA_I, %xmm8
	addps	-16 * SIZE(Y), %xmm4
	addps	%xmm8,   %xmm4
	movaps	%xmm4, -16 * SIZE(Y)

	movss	 %xmm6,  %xmm5
	shufps	 $0x93,  %xmm6, %xmm5
	pshufd	$0xb1,   %xmm5, %xmm8
	mulps	ALPHA_R, %xmm5
	mulps	ALPHA_I, %xmm8
	addps	-12 * SIZE(Y), %xmm5
	addps	%xmm8,   %xmm5
	movaps	%xmm5, -12 * SIZE(Y)

	movss	 %xmm7,  %xmm6
	shufps	 $0x93,  %xmm7, %xmm6
	pshufd	$0xb1,   %xmm6, %xmm8
	mulps	ALPHA_R, %xmm6
	mulps	ALPHA_I, %xmm8
	addps	 -8 * SIZE(Y), %xmm6
	addps	%xmm8,   %xmm6
	movaps	%xmm6,  -8 * SIZE(Y)

	movss	 %xmm0,  %xmm7
	shufps	 $0x93,  %xmm0, %xmm7
	pshufd	$0xb1,   %xmm7, %xmm8
	mulps	ALPHA_R, %xmm7
	mulps	ALPHA_I, %xmm8
	addps	 -4 * SIZE(Y), %xmm7
	addps	%xmm8,   %xmm7
	movaps	%xmm7,  -4 * SIZE(Y)

	subq	$-32 * SIZE, X
	subq	$-32 * SIZE, Y
	ALIGN_3

.L45:
	testq	$8, M
	jle	.L46

	movaps	-28 * SIZE(X), %xmm1
	movaps	-24 * SIZE(X), %xmm2

	movss	 %xmm1,  %xmm0
	shufps	 $0x93,  %xmm1, %xmm0
	pshufd	$0xb1,   %xmm0, %xmm8
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm8
	addps	-32 * SIZE(Y), %xmm0
	addps	%xmm8,   %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	movss	 %xmm2,  %xmm1
	shufps	 $0x93,  %xmm2, %xmm1
	pshufd	$0xb1,   %xmm1, %xmm8
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm8
	addps	-28 * SIZE(Y), %xmm1
	addps	%xmm8,   %xmm1
	movaps	%xmm1, -28 * SIZE(Y)

	movaps	-20 * SIZE(X), %xmm3
	movaps	-16 * SIZE(X), %xmm0

	movss	 %xmm3,  %xmm2
	shufps	 $0x93,  %xmm3, %xmm2
	pshufd	$0xb1,   %xmm2, %xmm8
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm8
	addps	-24 * SIZE(Y), %xmm2
	addps	%xmm8,   %xmm2
	movaps	%xmm2, -24 * SIZE(Y)

	movss	 %xmm0,  %xmm3
	shufps	 $0x93,  %xmm0, %xmm3
	pshufd	$0xb1,   %xmm3, %xmm8
	mulps	ALPHA_R, %xmm3
	mulps	ALPHA_I, %xmm8
	addps	-20 * SIZE(Y), %xmm3
	addps	%xmm8,   %xmm3
	movaps	%xmm3, -20 * SIZE(Y)

	addq	$16 * SIZE, X
	addq	$16 * SIZE, Y
	ALIGN_2

.L46:
	testq	$4, M
	jle	.L47

	movaps	-28 * SIZE(X), %xmm1
	movaps	-24 * SIZE(X), %xmm2

	movss	 %xmm1,  %xmm0
	shufps	 $0x93,  %xmm1, %xmm0
	pshufd	$0xb1,   %xmm0, %xmm8
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm8
	addps	-32 * SIZE(Y), %xmm0
	addps	%xmm8,   %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	movss	 %xmm2,  %xmm1
	shufps	 $0x93,  %xmm2, %xmm1
	pshufd	$0xb1,   %xmm1, %xmm8
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm8
	addps	-28 * SIZE(Y), %xmm1
	addps	%xmm8,   %xmm1
	movaps	%xmm1, -28 * SIZE(Y)

	movaps	%xmm2, %xmm0

	addq	$8 * SIZE, X
	addq	$8 * SIZE, Y
	ALIGN_2

.L47:
	testq	$2, M
	jle	.L48

	movaps	-28 * SIZE(X), %xmm1

	movss	 %xmm1,  %xmm0
	shufps	 $0x93,  %xmm1, %xmm0
	pshufd	$0xb1,   %xmm0, %xmm8
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm8

	addps	-32 * SIZE(Y), %xmm0
	addps	%xmm8,   %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	movaps	%xmm1, %xmm0

	addq	$4 * SIZE, X
	addq	$4 * SIZE, Y
	ALIGN_2

.L48:
	testq	$1, M
	jle	.L999

	movaps	-28 * SIZE(X), %xmm1
	movsd	-32 * SIZE(Y), %xmm2

	movss	 %xmm1,  %xmm0
	shufps	 $0x93,  %xmm1, %xmm0
	pshufd	$0xb1,   %xmm0, %xmm8
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm8

	addps	%xmm8,   %xmm0
	addps	%xmm2,   %xmm0
	movlps	%xmm0, -32 * SIZE(Y)

	jmp	.L999
	ALIGN_3
#endif

.L50:
	xorps	%xmm0, %xmm0

	subq	$1 * SIZE, Y

	testq	$3 * SIZE, X
	jne	.L60

	movq	M,  %rax
	sarq	$4, %rax
	jle	.L55

	movaps	-32 * SIZE(X), %xmm1
	movaps	-28 * SIZE(X), %xmm2
	movaps	-24 * SIZE(X), %xmm3
	movaps	-20 * SIZE(X), %xmm4

	decq	%rax
	jle	.L52
	ALIGN_3

.L51:
	movaps	-16 * SIZE(X), %xmm5
	movaps	-12 * SIZE(X), %xmm6

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	pshufd	$0xb1,   %xmm1, %xmm8
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm1
	movss	%xmm1,   %xmm0
	shufps	$0x93,   %xmm1, %xmm0
	addps	-32 * SIZE(Y), %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	pshufd	$0xb1,   %xmm2, %xmm8
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm2
	movss	%xmm2,   %xmm1
	shufps	$0x93,   %xmm2, %xmm1
	addps	-28 * SIZE(Y), %xmm1
	movaps	%xmm1, -28 * SIZE(Y)

	movaps	 -8 * SIZE(X), %xmm7
	movaps	 -4 * SIZE(X), %xmm0

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	pshufd	$0xb1,   %xmm3, %xmm8
	mulps	ALPHA_R, %xmm3
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm3
	movss	%xmm3,   %xmm2
	shufps	$0x93,   %xmm3, %xmm2
	addps	-24 * SIZE(Y), %xmm2
	movaps	%xmm2, -24 * SIZE(Y)

	pshufd	$0xb1,   %xmm4, %xmm8
	mulps	ALPHA_R, %xmm4
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm4
	movss	%xmm4,   %xmm3
	shufps	$0x93,   %xmm4, %xmm3
	addps	-20 * SIZE(Y), %xmm3
	movaps	%xmm3, -20 * SIZE(Y)

	movaps	  0 * SIZE(X), %xmm1
	movaps	  4 * SIZE(X), %xmm2

#if defined(PREFETCHW) && !defined(FETCH128)
	PREFETCHW (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	pshufd	$0xb1,   %xmm5, %xmm8
	mulps	ALPHA_R, %xmm5
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm5
	movss	%xmm5,   %xmm4
	shufps	$0x93,   %xmm5, %xmm4
	addps	-16 * SIZE(Y), %xmm4
	movaps	%xmm4, -16 * SIZE(Y)

	pshufd	$0xb1,   %xmm6, %xmm8
	mulps	ALPHA_R, %xmm6
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm6
	movss	%xmm6,   %xmm5
	shufps	$0x93,   %xmm6, %xmm5
	addps	-12 * SIZE(Y), %xmm5
	movaps	%xmm5, -12 * SIZE(Y)

	movaps	  8 * SIZE(X), %xmm3
	movaps	 12 * SIZE(X), %xmm4

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	pshufd	$0xb1,   %xmm7, %xmm8
	mulps	ALPHA_R, %xmm7
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm7
	movss	%xmm7,   %xmm6
	shufps	$0x93,   %xmm7, %xmm6
	addps	 -8 * SIZE(Y), %xmm6
	movaps	%xmm6,  -8 * SIZE(Y)

	pshufd	$0xb1,   %xmm0, %xmm8
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm0
	movss	%xmm0,   %xmm7
	shufps	$0x93,   %xmm0, %xmm7
	addps	 -4 * SIZE(Y), %xmm7
	movaps	%xmm7,  -4 * SIZE(Y)

	subq	$-32 * SIZE, X
	subq	$-32 * SIZE, Y
	decq	%rax
	jg	.L51
	ALIGN_3

.L52:
	movaps	-16 * SIZE(X), %xmm5
	movaps	-12 * SIZE(X), %xmm6

	pshufd	$0xb1,   %xmm1, %xmm8
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm1
	movss	%xmm1,   %xmm0
	shufps	$0x93,   %xmm1, %xmm0
	addps	-32 * SIZE(Y), %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	pshufd	$0xb1,   %xmm2, %xmm8
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm2
	movss	%xmm2,   %xmm1
	shufps	$0x93,   %xmm2, %xmm1
	addps	-28 * SIZE(Y), %xmm1
	movaps	%xmm1, -28 * SIZE(Y)

	movaps	 -8 * SIZE(X), %xmm7
	movaps	 -4 * SIZE(X), %xmm0

	pshufd	$0xb1,   %xmm3, %xmm8
	mulps	ALPHA_R, %xmm3
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm3
	movss	%xmm3,   %xmm2
	shufps	$0x93,   %xmm3, %xmm2
	addps	-24 * SIZE(Y), %xmm2
	movaps	%xmm2, -24 * SIZE(Y)

	pshufd	$0xb1,   %xmm4, %xmm8
	mulps	ALPHA_R, %xmm4
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm4
	movss	%xmm4,   %xmm3
	shufps	$0x93,   %xmm4, %xmm3
	addps	-20 * SIZE(Y), %xmm3
	movaps	%xmm3, -20 * SIZE(Y)

	pshufd	$0xb1,   %xmm5, %xmm8
	mulps	ALPHA_R, %xmm5
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm5
	movss	%xmm5,   %xmm4
	shufps	$0x93,   %xmm5, %xmm4
	addps	-16 * SIZE(Y), %xmm4
	movaps	%xmm4, -16 * SIZE(Y)

	pshufd	$0xb1,   %xmm6, %xmm8
	mulps	ALPHA_R, %xmm6
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm6
	movss	%xmm6,   %xmm5
	shufps	$0x93,   %xmm6, %xmm5
	addps	-12 * SIZE(Y), %xmm5
	movaps	%xmm5, -12 * SIZE(Y)

	pshufd	$0xb1,   %xmm7, %xmm8
	mulps	ALPHA_R, %xmm7
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm7
	movss	%xmm7,   %xmm6
	shufps	$0x93,   %xmm7, %xmm6
	addps	 -8 * SIZE(Y), %xmm6
	movaps	%xmm6,  -8 * SIZE(Y)

	pshufd	$0xb1,   %xmm0, %xmm8
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm0
	movss	%xmm0,   %xmm7
	shufps	$0x93,   %xmm0, %xmm7
	addps	 -4 * SIZE(Y), %xmm7
	movaps	%xmm7,  -4 * SIZE(Y)

	subq	$-32 * SIZE, X
	subq	$-32 * SIZE, Y
	ALIGN_3

.L55:
	testq	$8, M
	jle	.L56

	movaps	-32 * SIZE(X), %xmm1
	movaps	-28 * SIZE(X), %xmm2

	pshufd	$0xb1,   %xmm1, %xmm8
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm1
	movss	%xmm1,   %xmm0
	shufps	$0x93,   %xmm1, %xmm0
	addps	-32 * SIZE(Y), %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	pshufd	$0xb1,   %xmm2, %xmm8
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm2
	movss	%xmm2,   %xmm1
	shufps	$0x93,   %xmm2, %xmm1
	addps	-28 * SIZE(Y), %xmm1
	movaps	%xmm1, -28 * SIZE(Y)

	movaps	-24 * SIZE(X), %xmm3
	movaps	-20 * SIZE(X), %xmm0

	pshufd	$0xb1,   %xmm3, %xmm8
	mulps	ALPHA_R, %xmm3
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm3
	movss	%xmm3,   %xmm2
	shufps	$0x93,   %xmm3, %xmm2
	addps	-24 * SIZE(Y), %xmm2
	movaps	%xmm2, -24 * SIZE(Y)

	pshufd	$0xb1,   %xmm0, %xmm8
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm0
	movss	%xmm0,   %xmm3
	shufps	$0x93,   %xmm0, %xmm3
	addps	-20 * SIZE(Y), %xmm3
	movaps	%xmm3, -20 * SIZE(Y)

	addq	$16 * SIZE, X
	addq	$16 * SIZE, Y
	ALIGN_2

.L56:
	testq	$4, M
	jle	.L57

	movaps	-32 * SIZE(X), %xmm1
	movaps	-28 * SIZE(X), %xmm2

	pshufd	$0xb1,   %xmm1, %xmm8
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm1
	movss	%xmm1,   %xmm0
	shufps	$0x93,   %xmm1, %xmm0
	addps	-32 * SIZE(Y), %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	pshufd	$0xb1,   %xmm2, %xmm8
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm2
	movss	%xmm2,   %xmm1
	shufps	$0x93,   %xmm2, %xmm1
	addps	-28 * SIZE(Y), %xmm1
	movaps	%xmm1, -28 * SIZE(Y)

	movaps	%xmm2, %xmm0

	addq	$8 * SIZE, X
	addq	$8 * SIZE, Y
	ALIGN_2

.L57:
	testq	$2, M
	jle	.L58

	movaps	-32 * SIZE(X), %xmm1

	pshufd	$0xb1,   %xmm1, %xmm8
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm1
	movss	%xmm1,   %xmm0
	shufps	$0x93,   %xmm1, %xmm0

	addps	-32 * SIZE(Y), %xmm0
	movaps	%xmm0, -32 * SIZE(Y)
	movaps	%xmm1, %xmm0

	addq	$4 * SIZE, X
	addq	$4 * SIZE, Y
	ALIGN_2

.L58:
	testq	$1, M
	jle	.L59

#ifdef movsd
	xorps	%xmm1, %xmm1
#endif
	movsd	-32 * SIZE(X), %xmm1

	pshufd	$0xb1,   %xmm1, %xmm8
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm1
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

	testq	$2 * SIZE, X
	jne	.L70

	subq	$1 * SIZE, X

	movaps	-32 * SIZE(X), %xmm1

	movq	M,  %rax
	sarq	$4, %rax
	jle	.L65

	movaps	-28 * SIZE(X), %xmm2
	movaps	-24 * SIZE(X), %xmm3

	decq	%rax
	jle	.L62
	ALIGN_3

.L61:
	movaps	-20 * SIZE(X), %xmm4
	movaps	-16 * SIZE(X), %xmm5

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	movss	 %xmm2,  %xmm1
	SHUFPS_39  %xmm1, %xmm1
	pshufd	$0xb1,   %xmm1, %xmm8
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm1
	movss	%xmm1,   %xmm0
	shufps	$0x93,   %xmm1, %xmm0
	addps	-32 * SIZE(Y), %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	movss	 %xmm3,  %xmm2
	SHUFPS_39  %xmm2, %xmm2
	pshufd	$0xb1,   %xmm2, %xmm8
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm2
	movss	%xmm2,   %xmm1
	shufps	$0x93,   %xmm2, %xmm1
	addps	-28 * SIZE(Y), %xmm1
	movaps	%xmm1, -28 * SIZE(Y)

	movaps	-12 * SIZE(X), %xmm6
	movaps	 -8 * SIZE(X), %xmm7

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movss	 %xmm4,  %xmm3
	SHUFPS_39  %xmm3, %xmm3
	pshufd	$0xb1,   %xmm3, %xmm8
	mulps	ALPHA_R, %xmm3
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm3
	movss	%xmm3,   %xmm2
	shufps	$0x93,   %xmm3, %xmm2
	addps	-24 * SIZE(Y), %xmm2
	movaps	%xmm2, -24 * SIZE(Y)

	movss	 %xmm5,  %xmm4
	SHUFPS_39  %xmm4, %xmm4
	pshufd	$0xb1,   %xmm4, %xmm8
	mulps	ALPHA_R, %xmm4
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm4
	movss	%xmm4,   %xmm3
	shufps	$0x93,   %xmm4, %xmm3
	addps	-20 * SIZE(Y), %xmm3
	movaps	%xmm3, -20 * SIZE(Y)


#if defined(PREFETCHW) && !defined(FETCH128)
	PREFETCHW (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	movaps	 -4 * SIZE(X), %xmm0
	movaps	  0 * SIZE(X), %xmm1

	movss	 %xmm6,  %xmm5
	SHUFPS_39  %xmm5, %xmm5
	pshufd	$0xb1,   %xmm5, %xmm8
	mulps	ALPHA_R, %xmm5
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm5
	movss	%xmm5,   %xmm4
	shufps	$0x93,   %xmm5, %xmm4
	addps	-16 * SIZE(Y), %xmm4
	movaps	%xmm4, -16 * SIZE(Y)

	movss	 %xmm7,  %xmm6
	SHUFPS_39  %xmm6, %xmm6
	pshufd	$0xb1,   %xmm6, %xmm8
	mulps	ALPHA_R, %xmm6
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm6
	movss	%xmm6,   %xmm5
	shufps	$0x93,   %xmm6, %xmm5
	addps	-12 * SIZE(Y), %xmm5
	movaps	%xmm5, -12 * SIZE(Y)

	movaps	  4 * SIZE(X), %xmm2
	movaps	  8 * SIZE(X), %xmm3

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	movss	 %xmm0,  %xmm7
	SHUFPS_39  %xmm7, %xmm7
	pshufd	$0xb1,   %xmm7, %xmm8
	mulps	ALPHA_R, %xmm7
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm7
	movss	%xmm7,   %xmm6
	shufps	$0x93,   %xmm7, %xmm6
	addps	 -8 * SIZE(Y), %xmm6
	movaps	%xmm6,  -8 * SIZE(Y)

	movss	 %xmm1,  %xmm0
	SHUFPS_39  %xmm0, %xmm0
	pshufd	$0xb1,   %xmm0, %xmm8
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm0
	movss	%xmm0,   %xmm7
	shufps	$0x93,   %xmm0, %xmm7
	addps	 -4 * SIZE(Y), %xmm7
	movaps	%xmm7, -4 * SIZE(Y)

	subq	$-32 * SIZE, X
	subq	$-32 * SIZE, Y
	decq	%rax
	jg	.L61
	ALIGN_3

.L62:
	movaps	-20 * SIZE(X), %xmm4
	movaps	-16 * SIZE(X), %xmm5

	movss	 %xmm2,  %xmm1
	SHUFPS_39  %xmm1, %xmm1
	pshufd	$0xb1,   %xmm1, %xmm8
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm1
	movss	%xmm1,   %xmm0
	shufps	$0x93,   %xmm1, %xmm0
	addps	-32 * SIZE(Y), %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	movss	 %xmm3,  %xmm2
	SHUFPS_39  %xmm2, %xmm2
	pshufd	$0xb1,   %xmm2, %xmm8
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm2
	movss	%xmm2,   %xmm1
	shufps	$0x93,   %xmm2, %xmm1
	addps	-28 * SIZE(Y), %xmm1
	movaps	%xmm1, -28 * SIZE(Y)

	movaps	-12 * SIZE(X), %xmm6
	movaps	 -8 * SIZE(X), %xmm7

	movss	 %xmm4,  %xmm3
	SHUFPS_39  %xmm3, %xmm3
	pshufd	$0xb1,   %xmm3, %xmm8
	mulps	ALPHA_R, %xmm3
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm3
	movss	%xmm3,   %xmm2
	shufps	$0x93,   %xmm3, %xmm2
	addps	-24 * SIZE(Y), %xmm2
	movaps	%xmm2, -24 * SIZE(Y)

	movss	 %xmm5,  %xmm4
	SHUFPS_39  %xmm4, %xmm4
	pshufd	$0xb1,   %xmm4, %xmm8
	mulps	ALPHA_R, %xmm4
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm4
	movss	%xmm4,   %xmm3
	shufps	$0x93,   %xmm4, %xmm3
	addps	-20 * SIZE(Y), %xmm3
	movaps	%xmm3, -20 * SIZE(Y)

	movss	 %xmm6,  %xmm5
	SHUFPS_39  %xmm5, %xmm5
	pshufd	$0xb1,   %xmm5, %xmm8
	mulps	ALPHA_R, %xmm5
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm5
	movss	%xmm5,   %xmm4
	shufps	$0x93,   %xmm5, %xmm4
	addps	-16 * SIZE(Y), %xmm4
	movaps	%xmm4, -16 * SIZE(Y)

	movaps	 -4 * SIZE(X), %xmm0
	movaps	  0 * SIZE(X), %xmm1

	movss	 %xmm7,  %xmm6
	SHUFPS_39  %xmm6, %xmm6
	pshufd	$0xb1,   %xmm6, %xmm8
	mulps	ALPHA_R, %xmm6
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm6
	movss	%xmm6,   %xmm5
	shufps	$0x93,   %xmm6, %xmm5
	addps	-12 * SIZE(Y), %xmm5
	movaps	%xmm5, -12 * SIZE(Y)

	movss	 %xmm0,  %xmm7
	SHUFPS_39  %xmm7, %xmm7
	pshufd	$0xb1,   %xmm7, %xmm8
	mulps	ALPHA_R, %xmm7
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm7
	movss	%xmm7,   %xmm6
	shufps	$0x93,   %xmm7, %xmm6
	addps	 -8 * SIZE(Y), %xmm6
	movaps	%xmm6,  -8 * SIZE(Y)

	movss	 %xmm1,  %xmm0
	SHUFPS_39  %xmm0, %xmm0
	pshufd	$0xb1,   %xmm0, %xmm8
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm0
	movss	%xmm0,   %xmm7
	shufps	$0x93,   %xmm0, %xmm7
	addps	 -4 * SIZE(Y), %xmm7
	movaps	%xmm7, -4 * SIZE(Y)

	subq	$-32 * SIZE, X
	subq	$-32 * SIZE, Y
	ALIGN_3

.L65:
	testq	$8, M
	jle	.L66

	movaps	-28 * SIZE(X), %xmm2
	movaps	-24 * SIZE(X), %xmm3

	movss	 %xmm2,  %xmm1
	SHUFPS_39  %xmm1, %xmm1
	pshufd	$0xb1,   %xmm1, %xmm8
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm1
	movss	%xmm1,   %xmm0
	shufps	$0x93,   %xmm1, %xmm0
	addps	-32 * SIZE(Y), %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	movss	 %xmm3,  %xmm2
	SHUFPS_39  %xmm2, %xmm2
	pshufd	$0xb1,   %xmm2, %xmm8
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm2
	movss	%xmm2,   %xmm1
	shufps	$0x93,   %xmm2, %xmm1
	addps	-28 * SIZE(Y), %xmm1
	movaps	%xmm1, -28 * SIZE(Y)

	movaps	-20 * SIZE(X), %xmm0
	movaps	-16 * SIZE(X), %xmm1

	movss	 %xmm0,  %xmm3
	SHUFPS_39  %xmm3, %xmm3
	pshufd	$0xb1,   %xmm3, %xmm8
	mulps	ALPHA_R, %xmm3
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm3
	movss	%xmm3,   %xmm2
	shufps	$0x93,   %xmm3, %xmm2
	addps	-24 * SIZE(Y), %xmm2
	movaps	%xmm2, -24 * SIZE(Y)

	movss	 %xmm1,  %xmm0
	SHUFPS_39  %xmm0, %xmm0
	pshufd	$0xb1,   %xmm0, %xmm8
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm0
	movss	%xmm0,   %xmm3
	shufps	$0x93,   %xmm0, %xmm3
	addps	-20 * SIZE(Y), %xmm3
	movaps	%xmm3, -20 * SIZE(Y)

	addq	$16 * SIZE, X
	addq	$16 * SIZE, Y
	ALIGN_2

.L66:
	testq	$4, M
	jle	.L67

	movaps	-28 * SIZE(X), %xmm2
	movaps	-24 * SIZE(X), %xmm3

	movss	 %xmm2,  %xmm1
	SHUFPS_39  %xmm1, %xmm1
	pshufd	$0xb1,   %xmm1, %xmm8
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm1
	movss	%xmm1,   %xmm0
	shufps	$0x93,   %xmm1, %xmm0
	addps	-32 * SIZE(Y), %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	movss	 %xmm3,  %xmm2
	SHUFPS_39  %xmm2, %xmm2
	pshufd	$0xb1,   %xmm2, %xmm8
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm2
	movss	%xmm2,   %xmm1
	shufps	$0x93,   %xmm2, %xmm1
	addps	-28 * SIZE(Y), %xmm1
	movaps	%xmm1, -28 * SIZE(Y)

	movaps	%xmm2, %xmm0
	movaps	%xmm3, %xmm1

	addq	$8 * SIZE, X
	addq	$8 * SIZE, Y
	ALIGN_2

.L67:
	testq	$2, M
	jle	.L68

	movaps	-28 * SIZE(X), %xmm2

	movss	 %xmm2,  %xmm1
	SHUFPS_39  %xmm1, %xmm1
	pshufd	$0xb1,   %xmm1, %xmm8
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm1
	movss	%xmm1,   %xmm0
	shufps	$0x93,   %xmm1, %xmm0
	addps	-32 * SIZE(Y), %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	movaps	%xmm1, %xmm0
	movaps	%xmm2, %xmm1

	addq	$4 * SIZE, X
	addq	$4 * SIZE, Y
	ALIGN_2

.L68:
	testq	$1, M
	jle	.L69

	movaps	-28 * SIZE(X), %xmm2

	movss	 %xmm2,  %xmm1
	SHUFPS_39  %xmm1, %xmm1
	pshufd	$0xb1,   %xmm1, %xmm8
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm1
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
	testq	$1 * SIZE, X
	jne	.L80
#endif

	movq	M,  %rax
	sarq	$4, %rax
	jle	.L75

	movsd	-32 * SIZE(X), %xmm1
	movhps	-30 * SIZE(X), %xmm1
	movsd	-28 * SIZE(X), %xmm2
	movhps	-26 * SIZE(X), %xmm2
	movsd	-24 * SIZE(X), %xmm3
	movhps	-22 * SIZE(X), %xmm3
	movsd	-20 * SIZE(X), %xmm4
	movhps	-18 * SIZE(X), %xmm4

	decq	%rax
	jle	.L72
	ALIGN_3

.L71:
	movsd	-16 * SIZE(X), %xmm5
	movhps	-14 * SIZE(X), %xmm5
	movsd	-12 * SIZE(X), %xmm6
	movhps	-10 * SIZE(X), %xmm6

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	pshufd	$0xb1,   %xmm1, %xmm8
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm1
	movss	%xmm1,   %xmm0
	shufps	$0x93,   %xmm1, %xmm0
	addps	-32 * SIZE(Y), %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	pshufd	$0xb1,   %xmm2, %xmm8
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm2
	movss	%xmm2,   %xmm1
	shufps	$0x93,   %xmm2, %xmm1
	addps	-28 * SIZE(Y), %xmm1
	movaps	%xmm1, -28 * SIZE(Y)

	movsd	 -8 * SIZE(X), %xmm7
	movhps	 -6 * SIZE(X), %xmm7
	movsd	 -4 * SIZE(X), %xmm0
	movhps	 -2 * SIZE(X), %xmm0

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	pshufd	$0xb1,   %xmm3, %xmm8
	mulps	ALPHA_R, %xmm3
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm3
	movss	%xmm3,   %xmm2
	shufps	$0x93,   %xmm3, %xmm2
	addps	-24 * SIZE(Y), %xmm2
	movaps	%xmm2, -24 * SIZE(Y)

	pshufd	$0xb1,   %xmm4, %xmm8
	mulps	ALPHA_R, %xmm4
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm4
	movss	%xmm4,   %xmm3
	shufps	$0x93,   %xmm4, %xmm3
	addps	-20 * SIZE(Y), %xmm3
	movaps	%xmm3, -20 * SIZE(Y)

	movsd	  0 * SIZE(X), %xmm1
	movhps	  2 * SIZE(X), %xmm1
	movsd	  4 * SIZE(X), %xmm2
	movhps	  6 * SIZE(X), %xmm2

#if defined(PREFETCHW) && !defined(FETCH128)
	PREFETCHW (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	pshufd	$0xb1,   %xmm5, %xmm8
	mulps	ALPHA_R, %xmm5
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm5
	movss	%xmm5,   %xmm4
	shufps	$0x93,   %xmm5, %xmm4
	addps	-16 * SIZE(Y), %xmm4
	movaps	%xmm4, -16 * SIZE(Y)

	pshufd	$0xb1,   %xmm6, %xmm8
	mulps	ALPHA_R, %xmm6
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm6
	movss	%xmm6,   %xmm5
	shufps	$0x93,   %xmm6, %xmm5
	addps	-12 * SIZE(Y), %xmm5
	movaps	%xmm5, -12 * SIZE(Y)

	movsd	  8 * SIZE(X), %xmm3
	movhps	 10 * SIZE(X), %xmm3
	movsd	 12 * SIZE(X), %xmm4
	movhps	 14 * SIZE(X), %xmm4

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	pshufd	$0xb1,   %xmm7, %xmm8
	mulps	ALPHA_R, %xmm7
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm7
	movss	%xmm7,   %xmm6
	shufps	$0x93,   %xmm7, %xmm6
	addps	 -8 * SIZE(Y), %xmm6
	movaps	%xmm6,  -8 * SIZE(Y)

	pshufd	$0xb1,   %xmm0, %xmm8
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm0
	movss	%xmm0,   %xmm7
	shufps	$0x93,   %xmm0, %xmm7
	addps	 -4 * SIZE(Y), %xmm7
	movaps	%xmm7,  -4 * SIZE(Y)

	subq	$-32 * SIZE, X
	subq	$-32 * SIZE, Y
	decq	%rax
	jg	.L71
	ALIGN_3

.L72:
	movsd	-16 * SIZE(X), %xmm5
	movhps	-14 * SIZE(X), %xmm5
	movsd	-12 * SIZE(X), %xmm6
	movhps	-10 * SIZE(X), %xmm6

	pshufd	$0xb1,   %xmm1, %xmm8
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm1
	movss	%xmm1,   %xmm0
	shufps	$0x93,   %xmm1, %xmm0
	addps	-32 * SIZE(Y), %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	pshufd	$0xb1,   %xmm2, %xmm8
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm2
	movss	%xmm2,   %xmm1
	shufps	$0x93,   %xmm2, %xmm1
	addps	-28 * SIZE(Y), %xmm1
	movaps	%xmm1, -28 * SIZE(Y)

	movsd	 -8 * SIZE(X), %xmm7
	movhps	 -6 * SIZE(X), %xmm7
	movsd	 -4 * SIZE(X), %xmm0
	movhps	 -2 * SIZE(X), %xmm0

	pshufd	$0xb1,   %xmm3, %xmm8
	mulps	ALPHA_R, %xmm3
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm3
	movss	%xmm3,   %xmm2
	shufps	$0x93,   %xmm3, %xmm2
	addps	-24 * SIZE(Y), %xmm2
	movaps	%xmm2, -24 * SIZE(Y)

	pshufd	$0xb1,   %xmm4, %xmm8
	mulps	ALPHA_R, %xmm4
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm4
	movss	%xmm4,   %xmm3
	shufps	$0x93,   %xmm4, %xmm3
	addps	-20 * SIZE(Y), %xmm3
	movaps	%xmm3, -20 * SIZE(Y)

	pshufd	$0xb1,   %xmm5, %xmm8
	mulps	ALPHA_R, %xmm5
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm5
	movss	%xmm5,   %xmm4
	shufps	$0x93,   %xmm5, %xmm4
	addps	-16 * SIZE(Y), %xmm4
	movaps	%xmm4, -16 * SIZE(Y)

	pshufd	$0xb1,   %xmm6, %xmm8
	mulps	ALPHA_R, %xmm6
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm6
	movss	%xmm6,   %xmm5
	shufps	$0x93,   %xmm6, %xmm5
	addps	-12 * SIZE(Y), %xmm5
	movaps	%xmm5, -12 * SIZE(Y)

	pshufd	$0xb1,   %xmm7, %xmm8
	mulps	ALPHA_R, %xmm7
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm7
	movss	%xmm7,   %xmm6
	shufps	$0x93,   %xmm7, %xmm6
	addps	 -8 * SIZE(Y), %xmm6
	movaps	%xmm6,  -8 * SIZE(Y)

	pshufd	$0xb1,   %xmm0, %xmm8
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm0
	movss	%xmm0,   %xmm7
	shufps	$0x93,   %xmm0, %xmm7
	addps	 -4 * SIZE(Y), %xmm7
	movaps	%xmm7,  -4 * SIZE(Y)

	subq	$-32 * SIZE, X
	subq	$-32 * SIZE, Y
	ALIGN_3

.L75:
	testq	$8, M
	jle	.L76

	movsd	-32 * SIZE(X), %xmm1
	movhps	-30 * SIZE(X), %xmm1
	movsd	-28 * SIZE(X), %xmm2
	movhps	-26 * SIZE(X), %xmm2

	pshufd	$0xb1,   %xmm1, %xmm8
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm1
	movss	%xmm1,   %xmm0
	shufps	$0x93,   %xmm1, %xmm0
	addps	-32 * SIZE(Y), %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	pshufd	$0xb1,   %xmm2, %xmm8
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm2
	movss	%xmm2,   %xmm1
	shufps	$0x93,   %xmm2, %xmm1
	addps	-28 * SIZE(Y), %xmm1
	movaps	%xmm1, -28 * SIZE(Y)

	movsd	-24 * SIZE(X), %xmm3
	movhps	-22 * SIZE(X), %xmm3
	movsd	-20 * SIZE(X), %xmm0
	movhps	-18 * SIZE(X), %xmm0

	pshufd	$0xb1,   %xmm3, %xmm8
	mulps	ALPHA_R, %xmm3
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm3
	movss	%xmm3,   %xmm2
	shufps	$0x93,   %xmm3, %xmm2
	addps	-24 * SIZE(Y), %xmm2
	movaps	%xmm2, -24 * SIZE(Y)

	pshufd	$0xb1,   %xmm0, %xmm8
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm0
	movss	%xmm0,   %xmm3
	shufps	$0x93,   %xmm0, %xmm3
	addps	-20 * SIZE(Y), %xmm3
	movaps	%xmm3, -20 * SIZE(Y)

	addq	$16 * SIZE, X
	addq	$16 * SIZE, Y
	ALIGN_2

.L76:
	testq	$4, M
	jle	.L77

	movsd	-32 * SIZE(X), %xmm1
	movhps	-30 * SIZE(X), %xmm1

	pshufd	$0xb1,   %xmm1, %xmm8
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm1
	movss	%xmm1,   %xmm0
	shufps	$0x93,   %xmm1, %xmm0
	addps	-32 * SIZE(Y), %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	movsd	-28 * SIZE(X), %xmm2
	movhps	-26 * SIZE(X), %xmm2

	pshufd	$0xb1,   %xmm2, %xmm8
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm2
	movss	%xmm2,   %xmm1
	shufps	$0x93,   %xmm2, %xmm1
	addps	-28 * SIZE(Y), %xmm1
	movaps	%xmm1, -28 * SIZE(Y)

	movaps	%xmm2, %xmm0

	addq	$8 * SIZE, X
	addq	$8 * SIZE, Y
	ALIGN_2

.L77:
	testq	$2, M
	jle	.L78

	movsd	-32 * SIZE(X), %xmm1
	movhps	-30 * SIZE(X), %xmm1

	pshufd	$0xb1,   %xmm1, %xmm8
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm1
	movss	%xmm1,   %xmm0
	shufps	$0x93,   %xmm1, %xmm0

	addps	-32 * SIZE(Y), %xmm0
	movaps	%xmm0, -32 * SIZE(Y)
	movaps	%xmm1, %xmm0

	addq	$4 * SIZE, X
	addq	$4 * SIZE, Y
	ALIGN_2

.L78:
	testq	$1, M
	jle	.L79

#ifdef movsd
	xorps	%xmm1, %xmm1
#endif
	movsd	-32 * SIZE(X), %xmm1

	pshufd	$0xb1,   %xmm1, %xmm8
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm1
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
	subq	$3 * SIZE, X

	movaps	-32 * SIZE(X), %xmm1

	movq	M,  %rax
	sarq	$4, %rax
	jle	.L85

	movaps	-28 * SIZE(X), %xmm2
	movaps	-24 * SIZE(X), %xmm3

	decq	%rax
	jle	.L82
	ALIGN_3

.L81:
	movaps	-20 * SIZE(X), %xmm4
	movaps	-16 * SIZE(X), %xmm5

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	movss	 %xmm2,  %xmm1
	shufps	 $0x93,  %xmm2, %xmm1
	pshufd	$0xb1,   %xmm1, %xmm8
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm1
	movss	%xmm1,   %xmm0
	shufps	$0x93,   %xmm1, %xmm0
	addps	-32 * SIZE(Y), %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	movss	 %xmm3,  %xmm2
	shufps	 $0x93,  %xmm3, %xmm2
	pshufd	$0xb1,   %xmm2, %xmm8
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm2
	movss	%xmm2,   %xmm1
	shufps	$0x93,   %xmm2, %xmm1
	addps	-28 * SIZE(Y), %xmm1
	movaps	%xmm1, -28 * SIZE(Y)

	movaps	-12 * SIZE(X), %xmm6
	movaps	 -8 * SIZE(X), %xmm7

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movss	 %xmm4,  %xmm3
	shufps	 $0x93,  %xmm4, %xmm3
	pshufd	$0xb1,   %xmm3, %xmm8
	mulps	ALPHA_R, %xmm3
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm3
	movss	%xmm3,   %xmm2
	shufps	$0x93,   %xmm3, %xmm2
	addps	-24 * SIZE(Y), %xmm2
	movaps	%xmm2, -24 * SIZE(Y)

	movss	 %xmm5,  %xmm4
	shufps	 $0x93,  %xmm5, %xmm4
	pshufd	$0xb1,   %xmm4, %xmm8
	mulps	ALPHA_R, %xmm4
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm4
	movss	%xmm4,   %xmm3
	shufps	$0x93,   %xmm4, %xmm3
	addps	-20 * SIZE(Y), %xmm3
	movaps	%xmm3, -20 * SIZE(Y)

#if defined(PREFETCHW) && !defined(FETCH128)
	PREFETCHW (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	movaps	 -4 * SIZE(X), %xmm0
	movaps	  0 * SIZE(X), %xmm1

	movss	 %xmm6,  %xmm5
	shufps	 $0x93,  %xmm6, %xmm5
	pshufd	$0xb1,   %xmm5, %xmm8
	mulps	ALPHA_R, %xmm5
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm5
	movss	%xmm5,   %xmm4
	shufps	$0x93,   %xmm5, %xmm4
	addps	-16 * SIZE(Y), %xmm4
	movaps	%xmm4, -16 * SIZE(Y)

	movss	 %xmm7,  %xmm6
	shufps	 $0x93,  %xmm7, %xmm6
	pshufd	$0xb1,   %xmm6, %xmm8
	mulps	ALPHA_R, %xmm6
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm6
	movss	%xmm6,   %xmm5
	shufps	$0x93,   %xmm6, %xmm5
	addps	-12 * SIZE(Y), %xmm5
	movaps	%xmm5, -12 * SIZE(Y)

	movaps	  4 * SIZE(X), %xmm2
	movaps	  8 * SIZE(X), %xmm3

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	movss	 %xmm0,  %xmm7
	shufps	 $0x93,  %xmm0, %xmm7
	pshufd	$0xb1,   %xmm7, %xmm8
	mulps	ALPHA_R, %xmm7
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm7
	movss	%xmm7,   %xmm6
	shufps	$0x93,   %xmm7, %xmm6
	addps	 -8 * SIZE(Y), %xmm6
	movaps	%xmm6,  -8 * SIZE(Y)

	movss	 %xmm1,  %xmm0
	shufps	 $0x93,  %xmm1, %xmm0
	pshufd	$0xb1,   %xmm0, %xmm8
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm0
	movss	%xmm0,   %xmm7
	shufps	$0x93,   %xmm0, %xmm7
	addps	 -4 * SIZE(Y), %xmm7
	movaps	%xmm7, -4 * SIZE(Y)

	subq	$-32 * SIZE, X
	subq	$-32 * SIZE, Y
	decq	%rax
	jg	.L81
	ALIGN_3

.L82:
	movaps	-20 * SIZE(X), %xmm4
	movaps	-16 * SIZE(X), %xmm5

	movss	 %xmm2,  %xmm1
	shufps	 $0x93,  %xmm2, %xmm1
	pshufd	$0xb1,   %xmm1, %xmm8
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm1
	movss	%xmm1,   %xmm0
	shufps	$0x93,   %xmm1, %xmm0
	addps	-32 * SIZE(Y), %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	movss	 %xmm3,  %xmm2
	shufps	 $0x93,  %xmm3, %xmm2
	pshufd	$0xb1,   %xmm2, %xmm8
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm2
	movss	%xmm2,   %xmm1
	shufps	$0x93,   %xmm2, %xmm1
	addps	-28 * SIZE(Y), %xmm1
	movaps	%xmm1, -28 * SIZE(Y)

	movaps	-12 * SIZE(X), %xmm6
	movaps	 -8 * SIZE(X), %xmm7

	movss	 %xmm4,  %xmm3
	shufps	 $0x93,  %xmm4, %xmm3
	pshufd	$0xb1,   %xmm3, %xmm8
	mulps	ALPHA_R, %xmm3
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm3
	movss	%xmm3,   %xmm2
	shufps	$0x93,   %xmm3, %xmm2
	addps	-24 * SIZE(Y), %xmm2
	movaps	%xmm2, -24 * SIZE(Y)

	movss	 %xmm5,  %xmm4
	shufps	 $0x93,  %xmm5, %xmm4
	pshufd	$0xb1,   %xmm4, %xmm8
	mulps	ALPHA_R, %xmm4
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm4
	movss	%xmm4,   %xmm3
	shufps	$0x93,   %xmm4, %xmm3
	addps	-20 * SIZE(Y), %xmm3
	movaps	%xmm3, -20 * SIZE(Y)

	movss	 %xmm6,  %xmm5
	shufps	 $0x93,  %xmm6, %xmm5
	pshufd	$0xb1,   %xmm5, %xmm8
	mulps	ALPHA_R, %xmm5
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm5
	movss	%xmm5,   %xmm4
	shufps	$0x93,   %xmm5, %xmm4
	addps	-16 * SIZE(Y), %xmm4
	movaps	%xmm4, -16 * SIZE(Y)

	movaps	 -4 * SIZE(X), %xmm0
	movaps	  0 * SIZE(X), %xmm1

	movss	 %xmm7,  %xmm6
	shufps	 $0x93,  %xmm7, %xmm6
	pshufd	$0xb1,   %xmm6, %xmm8
	mulps	ALPHA_R, %xmm6
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm6
	movss	%xmm6,   %xmm5
	shufps	$0x93,   %xmm6, %xmm5
	addps	-12 * SIZE(Y), %xmm5
	movaps	%xmm5, -12 * SIZE(Y)

	movss	 %xmm0,  %xmm7
	shufps	 $0x93,  %xmm0, %xmm7
	pshufd	$0xb1,   %xmm7, %xmm8
	mulps	ALPHA_R, %xmm7
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm7
	movss	%xmm7,   %xmm6
	shufps	$0x93,   %xmm7, %xmm6
	addps	 -8 * SIZE(Y), %xmm6
	movaps	%xmm6,  -8 * SIZE(Y)

	movss	 %xmm1,  %xmm0
	shufps	 $0x93,  %xmm1, %xmm0
	pshufd	$0xb1,   %xmm0, %xmm8
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm0
	movss	%xmm0,   %xmm7
	shufps	$0x93,   %xmm0, %xmm7
	addps	 -4 * SIZE(Y), %xmm7
	movaps	%xmm7, -4 * SIZE(Y)

	subq	$-32 * SIZE, X
	subq	$-32 * SIZE, Y
	ALIGN_3

.L85:
	testq	$8, M
	jle	.L86

	movaps	-28 * SIZE(X), %xmm2
	movaps	-24 * SIZE(X), %xmm3

	movss	 %xmm2,  %xmm1
	shufps	 $0x93,  %xmm2, %xmm1
	pshufd	$0xb1,   %xmm1, %xmm8
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm1
	movss	%xmm1,   %xmm0
	shufps	$0x93,   %xmm1, %xmm0
	addps	-32 * SIZE(Y), %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	movss	 %xmm3,  %xmm2
	shufps	 $0x93,  %xmm3, %xmm2
	pshufd	$0xb1,   %xmm2, %xmm8
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm2
	movss	%xmm2,   %xmm1
	shufps	$0x93,   %xmm2, %xmm1
	addps	-28 * SIZE(Y), %xmm1
	movaps	%xmm1, -28 * SIZE(Y)

	movaps	-20 * SIZE(X), %xmm0
	movaps	-16 * SIZE(X), %xmm1

	movss	 %xmm0,  %xmm3
	shufps	 $0x93,  %xmm0, %xmm3
	pshufd	$0xb1,   %xmm3, %xmm8
	mulps	ALPHA_R, %xmm3
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm3
	movss	%xmm3,   %xmm2
	shufps	$0x93,   %xmm3, %xmm2
	addps	-24 * SIZE(Y), %xmm2
	movaps	%xmm2, -24 * SIZE(Y)

	movss	 %xmm1,  %xmm0
	shufps	 $0x93,  %xmm1, %xmm0
	pshufd	$0xb1,   %xmm0, %xmm8
	mulps	ALPHA_R, %xmm0
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm0
	movss	%xmm0,   %xmm3
	shufps	$0x93,   %xmm0, %xmm3
	addps	-20 * SIZE(Y), %xmm3
	movaps	%xmm3, -20 * SIZE(Y)

	addq	$16 * SIZE, X
	addq	$16 * SIZE, Y
	ALIGN_2

.L86:
	testq	$4, M
	jle	.L87

	movaps	-28 * SIZE(X), %xmm2
	movaps	-24 * SIZE(X), %xmm3

	movss	 %xmm2,  %xmm1
	shufps	 $0x93,  %xmm2, %xmm1
	pshufd	$0xb1,   %xmm1, %xmm8
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm1
	movss	%xmm1,   %xmm0
	shufps	$0x93,   %xmm1, %xmm0
	addps	-32 * SIZE(Y), %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	movss	 %xmm3,  %xmm2
	shufps	 $0x93,  %xmm3, %xmm2
	pshufd	$0xb1,   %xmm2, %xmm8
	mulps	ALPHA_R, %xmm2
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm2
	movss	%xmm2,   %xmm1
	shufps	$0x93,   %xmm2, %xmm1
	addps	-28 * SIZE(Y), %xmm1
	movaps	%xmm1, -28 * SIZE(Y)

	movaps	%xmm2, %xmm0
	movaps	%xmm3, %xmm1

	addq	$8 * SIZE, X
	addq	$8 * SIZE, Y
	ALIGN_2

.L87:
	testq	$2, M
	jle	.L88

	movaps	-28 * SIZE(X), %xmm2

	movss	 %xmm2,  %xmm1
	shufps	 $0x93,  %xmm2, %xmm1
	pshufd	$0xb1,   %xmm1, %xmm8
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm1
	movss	%xmm1,   %xmm0
	shufps	$0x93,   %xmm1, %xmm0
	addps	-32 * SIZE(Y), %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	movaps	%xmm1, %xmm0
	movaps	%xmm2, %xmm1

	addq	$4 * SIZE, X
	addq	$4 * SIZE, Y
	ALIGN_2

.L88:
	testq	$1, M
	jle	.L89

	movaps	-28 * SIZE(X), %xmm2

	movss	 %xmm2,  %xmm1
	shufps	 $0x93,  %xmm2, %xmm1
	pshufd	$0xb1,   %xmm1, %xmm8
	mulps	ALPHA_R, %xmm1
	mulps	ALPHA_I, %xmm8
	addps	%xmm8,   %xmm1
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
#ifndef CONJ
	pshufd	$0, %xmm0, %xmm14
	pshufd	$0, %xmm1, %xmm15

	pxor	%xmm13, %xmm13
	subps	%xmm15, %xmm13

	unpcklps %xmm14, %xmm13
	unpcklps %xmm15, %xmm14
	movaps	 %xmm13, %xmm15
#else
	pshufd	$0, %xmm0, %xmm14
	pshufd	$0, %xmm1, %xmm15

	pxor	%xmm13, %xmm13
	subps	%xmm14, %xmm13

	unpcklps %xmm15, %xmm14
	unpcklps %xmm13, %xmm15
#endif

//If incx==0 || incy==0, avoid unloop and jump to end.
	cmpq	$0, INCX
	je  .L200
	cmpq	$0, INCY
	je	.L200

	movq	Y, YY

	movq	M,  %rax
	sarq	$3, %rax
	jle	.L105
	ALIGN_3

.L102:
	movsd	 (X), %xmm0
	addq	 INCX, X
	movhps	 (X), %xmm0
	addq	 INCX, X
	movsd	 (X), %xmm2
	addq	 INCX, X
	movhps	 (X), %xmm2
	addq	 INCX, X
	movsd	 (X), %xmm4
	addq	 INCX, X
	movhps	 (X), %xmm4
	addq	 INCX, X
	movsd	 (X), %xmm6
	addq	 INCX, X
	movhps	 (X), %xmm6
	addq	 INCX, X

#ifdef HAVE_SSE3
	movshdup %xmm0, %xmm1
	movsldup %xmm0, %xmm0
	movshdup %xmm2, %xmm3
	movsldup %xmm2, %xmm2
	movshdup %xmm4, %xmm5
	movsldup %xmm4, %xmm4
	movshdup %xmm6, %xmm7
	movsldup %xmm6, %xmm6
#else
	pshufd	$0xf5, %xmm0, %xmm1
	shufps	$0xa0, %xmm0, %xmm0
	pshufd	$0xf5, %xmm2, %xmm3
	shufps	$0xa0, %xmm2, %xmm2
	pshufd	$0xf5, %xmm4, %xmm5
	shufps	$0xa0, %xmm4, %xmm4
	pshufd	$0xf5, %xmm6, %xmm7
	shufps	$0xa0, %xmm6, %xmm6
#endif

	mulps	%xmm14, %xmm0
	mulps	%xmm15, %xmm1
	mulps	%xmm14, %xmm2
	mulps	%xmm15, %xmm3
	mulps	%xmm14, %xmm4
	mulps	%xmm15, %xmm5
	mulps	%xmm14, %xmm6
	mulps	%xmm15, %xmm7

	movsd	(Y), %xmm8
	addq	 INCY, Y
	movhps	(Y), %xmm8
	addq	 INCY, Y
	movsd	(Y), %xmm9
	addq	 INCY, Y
	movhps	(Y), %xmm9
	addq	 INCY, Y
	movsd	(Y), %xmm10
	addq	 INCY, Y
	movhps	(Y), %xmm10
	addq	 INCY, Y
	movsd	(Y), %xmm11
	addq	 INCY, Y
	movhps	(Y), %xmm11
	addq	 INCY, Y

	addps	%xmm0, %xmm8
	addps	%xmm1, %xmm8
	addps	%xmm2, %xmm9
	addps	%xmm3, %xmm9
	addps	%xmm4, %xmm10
	addps	%xmm5, %xmm10
	addps	%xmm6, %xmm11
	addps	%xmm7, %xmm11

	movsd	%xmm8,  (YY)
	addq	 INCY, YY
	movhps	%xmm8,  (YY)
	addq	 INCY, YY
	movsd	%xmm9,  (YY)
	addq	 INCY, YY
	movhps	%xmm9,  (YY)
	addq	 INCY, YY
	movsd	%xmm10, (YY)
	addq	 INCY, YY
	movhps	%xmm10, (YY)
	addq	 INCY, YY
	movsd	%xmm11, (YY)
	addq	 INCY, YY
	movhps	%xmm11, (YY)
	addq	 INCY, YY

	decq	%rax
	jg	.L102
	ALIGN_3

.L105:
	testq	$4, M
	jle	.L106

	movsd	 (X), %xmm0
	addq	 INCX, X
	movhps	 (X), %xmm0
	addq	 INCX, X
	movsd	 (X), %xmm2
	addq	 INCX, X
	movhps	 (X), %xmm2
	addq	 INCX, X

#ifdef HAVE_SSE3
	movshdup %xmm0, %xmm1
	movsldup %xmm0, %xmm0
	movshdup %xmm2, %xmm3
	movsldup %xmm2, %xmm2
#else
	pshufd	$0xf5, %xmm0, %xmm1
	shufps	$0xa0, %xmm0, %xmm0
	pshufd	$0xf5, %xmm2, %xmm3
	shufps	$0xa0, %xmm2, %xmm2
#endif

	mulps	%xmm14, %xmm0
	mulps	%xmm15, %xmm1
	mulps	%xmm14, %xmm2
	mulps	%xmm15, %xmm3

	movsd	(Y), %xmm8
	addq	 INCY, Y
	movhps	(Y), %xmm8
	addq	 INCY, Y
	movsd	(Y), %xmm9
	addq	 INCY, Y
	movhps	(Y), %xmm9
	addq	 INCY, Y

	addps	%xmm0, %xmm8
	addps	%xmm1, %xmm8
	addps	%xmm2, %xmm9
	addps	%xmm3, %xmm9

	movsd	%xmm8,  (YY)
	addq	 INCY, YY
	movhps	%xmm8,  (YY)
	addq	 INCY, YY
	movsd	%xmm9,  (YY)
	addq	 INCY, YY
	movhps	%xmm9,  (YY)
	addq	 INCY, YY
	ALIGN_3

.L106:
	testq	$2, M
	jle	.L107

	movsd	 (X), %xmm0
	addq	 INCX, X
	movhps	 (X), %xmm0
	addq	 INCX, X

#ifdef HAVE_SSE3
	movshdup %xmm0, %xmm1
	movsldup %xmm0, %xmm0
#else
	pshufd	$0xf5, %xmm0, %xmm1
	shufps	$0xa0, %xmm0, %xmm0
#endif

	mulps	%xmm14, %xmm0
	mulps	%xmm15, %xmm1

	movsd	(Y), %xmm8
	addq	 INCY, Y
	movhps	(Y), %xmm8
	addq	 INCY, Y

	addps	%xmm0, %xmm8
	addps	%xmm1, %xmm8

	movsd	%xmm8,  (YY)
	addq	 INCY, YY
	movhps	%xmm8,  (YY)
	addq	 INCY, YY
	ALIGN_3

.L107:
	testq	$1, M
	jle	.L999

	movsd	 (X), %xmm0

#ifdef HAVE_SSE3
	movshdup %xmm0, %xmm1
	movsldup %xmm0, %xmm0
#else
	pshufd	$0xf5, %xmm0, %xmm1
	shufps	$0xa0, %xmm0, %xmm0
#endif

	mulps	%xmm14, %xmm0
	mulps	%xmm15, %xmm1

	movsd	 (Y), %xmm8

	addps	%xmm0, %xmm8
	addps	%xmm1, %xmm8

	movsd	%xmm8,   (Y)
	jmp	.L999
	ALIGN_3

.L200:
    movq    M, %rax
	cmpq	$0, %rax
    jle .L999
    ALIGN_3

.L201:
    movsd    (X), %xmm0
    addq    INCX, X

#ifdef HAVE_SSE3
    movshdup %xmm0, %xmm1
    movsldup %xmm0, %xmm0
#else
    pshufd  $0xf5, %xmm0, %xmm1
    shufps  $0xa0, %xmm0, %xmm0
#endif

    mulps   %xmm14, %xmm0
    mulps   %xmm15, %xmm1

    movsd    (Y), %xmm8

    addps   %xmm0, %xmm8
    addps   %xmm1, %xmm8

	movsd   %xmm8,   (Y)
    addq    INCY, Y

	decq    %rax
    jg  .L201
    ALIGN_3

.L999:
	xorq	%rax, %rax

	RESTOREREGISTERS

	ret

	EPILOGUE

