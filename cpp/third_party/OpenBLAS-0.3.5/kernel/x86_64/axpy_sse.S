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
#define ALPHA	%xmm15

#include "l1param.h"

	PROLOGUE
	PROFCODE

#ifndef WINDOWS_ABI
#ifndef XDOUBLE
	movq	 8(%rsp), INCY
#else
	movq	24(%rsp), INCY
#endif
	movaps	%xmm0,  ALPHA
#else


	movq	40(%rsp), X
	movq	48(%rsp), INCX
	movq	56(%rsp), Y
	movq	64(%rsp), INCY
#endif

	SAVEREGISTERS

#ifdef WINDOWS_ABI
	movaps	%xmm3,  ALPHA
#endif

	shufps	$0, ALPHA, ALPHA

	leaq	(, INCX, SIZE), INCX
	leaq	(, INCY, SIZE), INCY

	testq	M, M
	jle	.L19

	cmpq	$SIZE, INCX
	jne	.L50
	cmpq	$SIZE, INCY
	jne	.L50

	subq	$-32 * SIZE, X
	subq	$-32 * SIZE, Y

	cmpq	$3, M
	jle	.L16

	testq	$SIZE, Y
	je	.L00

	movss	-32 * SIZE(X), %xmm0
	mulss	ALPHA, %xmm0
	addss	-32 * SIZE(Y), %xmm0
	movss	%xmm0, -32 * SIZE(Y)
	addq	$1 * SIZE, X
	addq	$1 * SIZE, Y
	decq	M
	jle	.L19
	ALIGN_3

.L00:
	testq	$SIZE * 2, Y
	je	.L10

	movsd	-32 * SIZE(X), %xmm0
	movsd	-32 * SIZE(Y), %xmm4
	mulps	ALPHA, %xmm0
	addps	%xmm4, %xmm0
	movsd	%xmm0, -32 * SIZE(Y)

	addq	$2 * SIZE, X
	addq	$2 * SIZE, Y
	subq	$2, M
	jle	.L19
	ALIGN_3

.L10:
	testq	$SIZE * 3, X
	jne	.L20

	movq	M,  %rax
	sarq	$5, %rax
	jle	.L13

	movaps	-32 * SIZE(X), %xmm0
	movaps	-28 * SIZE(X), %xmm1
	movaps	-24 * SIZE(X), %xmm2
	movaps	-20 * SIZE(X), %xmm3

	decq	%rax
	jle .L12
	ALIGN_4

.L11:
	movaps	-16 * SIZE(X), %xmm4
	movaps	-12 * SIZE(X), %xmm5

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	mulps	ALPHA, %xmm0
	addps	-32 * SIZE(Y), %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	mulps	ALPHA, %xmm1
	addps	-28 * SIZE(Y), %xmm1
	movaps	%xmm1, -28 * SIZE(Y)

	movaps	 -8 * SIZE(X), %xmm6
	movaps	 -4 * SIZE(X), %xmm7

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	mulps	ALPHA, %xmm2
	addps	-24 * SIZE(Y), %xmm2
	movaps	%xmm2, -24 * SIZE(Y)

	mulps	ALPHA, %xmm3
	addps	-20 * SIZE(Y), %xmm3
	movaps	%xmm3, -20 * SIZE(Y)

	movaps	 0 * SIZE(X), %xmm0
	movaps	 4 * SIZE(X), %xmm1

#if defined(PREFETCHW) && !defined(FETCH128)
	PREFETCHW (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	mulps	ALPHA, %xmm4
	addps	-16 * SIZE(Y), %xmm4
	movaps	%xmm4, -16 * SIZE(Y)

	mulps	ALPHA, %xmm5
	addps	-12 * SIZE(Y), %xmm5
	movaps	%xmm5, -12 * SIZE(Y)

	movaps	 8 * SIZE(X), %xmm2
	movaps	12 * SIZE(X), %xmm3

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	mulps	ALPHA, %xmm6
	addps	 -8 * SIZE(Y), %xmm6
	movaps	%xmm6,  -8 * SIZE(Y)

	mulps	ALPHA, %xmm7
	addps	 -4 * SIZE(Y), %xmm7
	movaps	%xmm7,  -4 * SIZE(Y)

	subq	$-32 * SIZE, X
	subq	$-32 * SIZE, Y
	decq	%rax
	jg	.L11
	ALIGN_3

.L12:
	movaps	-16 * SIZE(X), %xmm4
	movaps	-12 * SIZE(X), %xmm5

	mulps	ALPHA, %xmm0
	addps	-32 * SIZE(Y), %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	mulps	ALPHA, %xmm1
	addps	-28 * SIZE(Y), %xmm1
	movaps	%xmm1, -28 * SIZE(Y)

	movaps	 -8 * SIZE(X), %xmm6
	movaps	 -4 * SIZE(X), %xmm7

	mulps	ALPHA, %xmm2
	addps	-24 * SIZE(Y), %xmm2
	movaps	%xmm2, -24 * SIZE(Y)

	mulps	ALPHA, %xmm3
	addps	-20 * SIZE(Y), %xmm3
	movaps	%xmm3, -20 * SIZE(Y)

	mulps	ALPHA, %xmm4
	addps	-16 * SIZE(Y), %xmm4
	movaps	%xmm4, -16 * SIZE(Y)

	mulps	ALPHA, %xmm5
	addps	-12 * SIZE(Y), %xmm5
	movaps	%xmm5, -12 * SIZE(Y)

	mulps	ALPHA, %xmm6
	addps	 -8 * SIZE(Y), %xmm6
	movaps	%xmm6,  -8 * SIZE(Y)

	mulps	ALPHA, %xmm7
	addps	 -4 * SIZE(Y), %xmm7
	movaps	%xmm7,  -4 * SIZE(Y)

	subq	$-32 * SIZE, X
	subq	$-32 * SIZE, Y
	ALIGN_3

.L13:
	movq	M,  %rax
	andq	$16, %rax
	jle	.L14
	ALIGN_3

	movaps	-32 * SIZE(X), %xmm0
	movaps	-28 * SIZE(X), %xmm1
	movaps	-24 * SIZE(X), %xmm2
	movaps	-20 * SIZE(X), %xmm3

	mulps	ALPHA, %xmm0
	addps	-32 * SIZE(Y), %xmm0
	mulps	ALPHA, %xmm1
	addps	-28 * SIZE(Y), %xmm1
	mulps	ALPHA, %xmm2
	addps	-24 * SIZE(Y), %xmm2
	mulps	ALPHA, %xmm3
	addps	-20 * SIZE(Y), %xmm3

	movaps	%xmm0, -32 * SIZE(Y)
	movaps	%xmm1, -28 * SIZE(Y)
	movaps	%xmm2, -24 * SIZE(Y)
	movaps	%xmm3, -20 * SIZE(Y)

	addq	$16 * SIZE, X
	addq	$16 * SIZE, Y
	ALIGN_3

.L14:
	movq	M,  %rax
	andq	$8, %rax
	jle	.L15
	ALIGN_3

	movaps	-32 * SIZE(X), %xmm0
	movaps	-28 * SIZE(X), %xmm1

	mulps	ALPHA, %xmm0
	addps	-32 * SIZE(Y), %xmm0
	mulps	ALPHA, %xmm1
	addps	-28 * SIZE(Y), %xmm1

	movaps	%xmm0, -32 * SIZE(Y)
	movaps	%xmm1, -28 * SIZE(Y)

	addq	$8 * SIZE, X
	addq	$8 * SIZE, Y
	ALIGN_3

.L15:
	movq	M,  %rax
	andq	$4, %rax
	jle	.L16
	ALIGN_3

	movaps	-32 * SIZE(X), %xmm0

	mulps	ALPHA, %xmm0

	addps	-32 * SIZE(Y), %xmm0

	movaps	%xmm0, -32 * SIZE(Y)

	addq	$4 * SIZE, X
	addq	$4 * SIZE, Y
	ALIGN_3

.L16:
	movq	M,  %rax
	andq	$2, %rax
	jle	.L17
	ALIGN_3

	movsd	-32 * SIZE(X), %xmm0
	movsd	-32 * SIZE(Y), %xmm4

	mulps	ALPHA, %xmm0
	addps	%xmm4, %xmm0

	movsd	%xmm0, -32 * SIZE(Y)

	addq	$2 * SIZE, X
	addq	$2 * SIZE, Y
	ALIGN_3

.L17:
	movq	M,  %rax
	andq	$1, %rax
	jle	.L19
	ALIGN_3

	movss	-32 * SIZE(X), %xmm0
	mulss	ALPHA, %xmm0
	addss	-32 * SIZE(Y), %xmm0

	movss	%xmm0, 	-32 * SIZE(Y)
	ALIGN_3

.L19:
	xorq	%rax,%rax

	RESTOREREGISTERS

	ret
	ALIGN_3

.L20:

#ifdef ALIGNED_ACCESS

	testq	$SIZE, X
	jne	.L30

	movhps	-32 * SIZE(X), %xmm0

	movq	M,  %rax
	sarq	$5, %rax
	jle	.L23

	movaps	-30 * SIZE(X), %xmm1
	movaps	-26 * SIZE(X), %xmm2
	movaps	-22 * SIZE(X), %xmm3
	movaps	-18 * SIZE(X), %xmm4

	decq	%rax
	jle .L22
	ALIGN_4

.L21:
	movaps	-14 * SIZE(X), %xmm5
	movaps	-10 * SIZE(X), %xmm6

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	SHUFPD_1 %xmm1, %xmm0
	mulps	ALPHA, %xmm0
	addps	-32 * SIZE(Y), %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	SHUFPD_1 %xmm2, %xmm1
	mulps	ALPHA, %xmm1
	addps	-28 * SIZE(Y), %xmm1
	movaps	%xmm1, -28 * SIZE(Y)

	movaps	 -6 * SIZE(X), %xmm7
	movaps	 -2 * SIZE(X), %xmm0

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	SHUFPD_1 %xmm3, %xmm2
	mulps	ALPHA, %xmm2
	addps	-24 * SIZE(Y), %xmm2
	movaps	%xmm2, -24 * SIZE(Y)

	SHUFPD_1 %xmm4, %xmm3
	mulps	ALPHA, %xmm3
	addps	-20 * SIZE(Y), %xmm3
	movaps	%xmm3, -20 * SIZE(Y)

	movaps	  2 * SIZE(X), %xmm1
	movaps	  6 * SIZE(X), %xmm2

#if defined(PREFETCHW) && !defined(FETCH128)
	PREFETCHW (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	SHUFPD_1 %xmm5, %xmm4
	mulps	ALPHA, %xmm4
	addps	-16 * SIZE(Y), %xmm4
	movaps	%xmm4, -16 * SIZE(Y)

	SHUFPD_1 %xmm6, %xmm5
	mulps	ALPHA, %xmm5
	addps	-12 * SIZE(Y), %xmm5
	movaps	%xmm5, -12 * SIZE(Y)

	movaps	 10 * SIZE(X), %xmm3
	movaps	 14 * SIZE(X), %xmm4

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	SHUFPD_1 %xmm7, %xmm6
	mulps	ALPHA, %xmm6
	addps	 -8 * SIZE(Y), %xmm6
	movaps	%xmm6,  -8 * SIZE(Y)

	SHUFPD_1 %xmm0, %xmm7
	mulps	ALPHA, %xmm7
	addps	 -4 * SIZE(Y), %xmm7
	movaps	%xmm7,  -4 * SIZE(Y)

	subq	$-32 * SIZE, X
	subq	$-32 * SIZE, Y
	decq	%rax
	jg	.L21
	ALIGN_3

.L22:
	movaps	-14 * SIZE(X), %xmm5
	movaps	-10 * SIZE(X), %xmm6

	SHUFPD_1 %xmm1, %xmm0
	mulps	ALPHA, %xmm0
	addps	-32 * SIZE(Y), %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	SHUFPD_1 %xmm2, %xmm1
	mulps	ALPHA, %xmm1
	addps	-28 * SIZE(Y), %xmm1
	movaps	%xmm1, -28 * SIZE(Y)

	movaps	 -6 * SIZE(X), %xmm7
	movaps	 -2 * SIZE(X), %xmm0

	SHUFPD_1 %xmm3, %xmm2
	mulps	ALPHA, %xmm2
	addps	-24 * SIZE(Y), %xmm2
	movaps	%xmm2, -24 * SIZE(Y)

	SHUFPD_1 %xmm4, %xmm3
	mulps	ALPHA, %xmm3
	addps	-20 * SIZE(Y), %xmm3
	movaps	%xmm3, -20 * SIZE(Y)

	SHUFPD_1 %xmm5, %xmm4
	mulps	ALPHA, %xmm4
	addps	-16 * SIZE(Y), %xmm4
	movaps	%xmm4, -16 * SIZE(Y)

	SHUFPD_1 %xmm6, %xmm5
	mulps	ALPHA, %xmm5
	addps	-12 * SIZE(Y), %xmm5
	movaps	%xmm5, -12 * SIZE(Y)

	SHUFPD_1 %xmm7, %xmm6
	mulps	ALPHA, %xmm6
	addps	 -8 * SIZE(Y), %xmm6
	movaps	%xmm6,  -8 * SIZE(Y)

	SHUFPD_1 %xmm0, %xmm7
	mulps	ALPHA, %xmm7
	addps	 -4 * SIZE(Y), %xmm7
	movaps	%xmm7,  -4 * SIZE(Y)

	subq	$-32 * SIZE, X
	subq	$-32 * SIZE, Y
	ALIGN_3

.L23:
	movq	M,  %rax
	andq	$16, %rax
	jle	.L24
	ALIGN_3

	movaps	-30 * SIZE(X), %xmm1
	movaps	-26 * SIZE(X), %xmm2
	movaps	-22 * SIZE(X), %xmm3
	movaps	-18 * SIZE(X), %xmm4

	SHUFPD_1 %xmm1, %xmm0
	SHUFPD_1 %xmm2, %xmm1
	SHUFPD_1 %xmm3, %xmm2
	SHUFPD_1 %xmm4, %xmm3

	mulps	ALPHA, %xmm0
	addps	-32 * SIZE(Y), %xmm0
	mulps	ALPHA, %xmm1
	addps	-28 * SIZE(Y), %xmm1
	mulps	ALPHA, %xmm2
	addps	-24 * SIZE(Y), %xmm2
	mulps	ALPHA, %xmm3
	addps	-20 * SIZE(Y), %xmm3

	movaps	%xmm0, -32 * SIZE(Y)
	movaps	%xmm1, -28 * SIZE(Y)
	movaps	%xmm2, -24 * SIZE(Y)
	movaps	%xmm3, -20 * SIZE(Y)

	movaps	%xmm4, %xmm0

	addq	$16 * SIZE, X
	addq	$16 * SIZE, Y
	ALIGN_3

.L24:
	movq	M,  %rax
	andq	$8, %rax
	jle	.L25
	ALIGN_3

	movaps	-30 * SIZE(X), %xmm1
	movaps	-26 * SIZE(X), %xmm2

	SHUFPD_1 %xmm1, %xmm0
	mulps	ALPHA, %xmm0
	addps	-32 * SIZE(Y), %xmm0
	SHUFPD_1 %xmm2, %xmm1
	mulps	ALPHA, %xmm1
	addps	-28 * SIZE(Y), %xmm1

	movaps	%xmm0, -32 * SIZE(Y)
	movaps	%xmm1, -28 * SIZE(Y)
	movaps	%xmm2, %xmm0

	addq	$8 * SIZE, X
	addq	$8 * SIZE, Y
	ALIGN_3

.L25:
	movq	M,  %rax
	andq	$4, %rax
	jle	.L26
	ALIGN_3

	movaps	-30 * SIZE(X), %xmm1

	SHUFPD_1 %xmm1, %xmm0
	mulps	ALPHA, %xmm0

	addps	-32 * SIZE(Y), %xmm0

	movaps	%xmm0, -32 * SIZE(Y)

	addq	$4 * SIZE, X
	addq	$4 * SIZE, Y
	ALIGN_3

.L26:
	movq	M,  %rax
	andq	$2, %rax
	jle	.L27
	ALIGN_3

	movsd	-32 * SIZE(X), %xmm0
	movsd	-32 * SIZE(Y), %xmm4

	mulps	ALPHA, %xmm0
	addps	%xmm4, %xmm0

	movsd	%xmm0, -32 * SIZE(Y)

	addq	$2 * SIZE, X
	addq	$2 * SIZE, Y
	ALIGN_3

.L27:
	movq	M,  %rax
	andq	$1, %rax
	jle	.L29
	ALIGN_3

	movss	-32 * SIZE(X), %xmm0
	mulss	ALPHA, %xmm0
	addss	-32 * SIZE(Y), %xmm0

	movss	%xmm0, 	-32 * SIZE(Y)
	addq	$SIZE, Y
	ALIGN_3

.L29:
	xorq	%rax,%rax

	RESTOREREGISTERS

	ret
	ALIGN_3

.L30:
	testq	$2 * SIZE, X
	jne	.L40

	movaps	-33 * SIZE(X), %xmm0

	movq	M,  %rax
	sarq	$5, %rax
	jle	.L33

	movaps	-29 * SIZE(X), %xmm1
	movaps	-25 * SIZE(X), %xmm2
	movaps	-21 * SIZE(X), %xmm3
	movaps	-17 * SIZE(X), %xmm4

	decq	%rax
	jle .L32
	ALIGN_4

.L31:
	movaps	-13 * SIZE(X), %xmm5
	movaps	 -9 * SIZE(X), %xmm6

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	movss	%xmm1, %xmm0
	SHUFPS_39 %xmm0, %xmm0
	mulps	ALPHA, %xmm0
	addps	-32 * SIZE(Y), %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	movss	%xmm2, %xmm1
	SHUFPS_39 %xmm1, %xmm1
	mulps	ALPHA, %xmm1
	addps	-28 * SIZE(Y), %xmm1
	movaps	%xmm1, -28 * SIZE(Y)

	movaps	 -5 * SIZE(X), %xmm7
	movaps	 -1 * SIZE(X), %xmm0

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movss	%xmm3, %xmm2
	SHUFPS_39 %xmm2, %xmm2
	mulps	ALPHA, %xmm2
	addps	-24 * SIZE(Y), %xmm2
	movaps	%xmm2, -24 * SIZE(Y)

	movss	%xmm4, %xmm3
	SHUFPS_39 %xmm3, %xmm3
	mulps	ALPHA, %xmm3
	addps	-20 * SIZE(Y), %xmm3
	movaps	%xmm3, -20 * SIZE(Y)

	movaps	  3 * SIZE(X), %xmm1
	movaps	  7 * SIZE(X), %xmm2

#if defined(PREFETCHW) && !defined(FETCH128)
	PREFETCHW (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	movss	%xmm5, %xmm4
	SHUFPS_39 %xmm4, %xmm4
	mulps	ALPHA, %xmm4
	addps	-16 * SIZE(Y), %xmm4
	movaps	%xmm4, -16 * SIZE(Y)

	movss	%xmm6, %xmm5
	SHUFPS_39 %xmm5, %xmm5
	mulps	ALPHA, %xmm5
	addps	-12 * SIZE(Y), %xmm5
	movaps	%xmm5, -12 * SIZE(Y)

	movaps	 11 * SIZE(X), %xmm3
	movaps	 15 * SIZE(X), %xmm4

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	movss	%xmm7, %xmm6
	SHUFPS_39 %xmm6, %xmm6
	mulps	ALPHA, %xmm6
	addps	-8 * SIZE(Y), %xmm6
	movaps	%xmm6,  -8 * SIZE(Y)

	movss	%xmm0, %xmm7
	SHUFPS_39 %xmm7, %xmm7
	mulps	ALPHA, %xmm7
	addps	-4 * SIZE(Y), %xmm7
	movaps	%xmm7,  -4 * SIZE(Y)

	subq	$-32 * SIZE, X
	subq	$-32 * SIZE, Y
	decq	%rax
	jg	.L31
	ALIGN_3

.L32:
	movaps	-13 * SIZE(X), %xmm5
	movaps	 -9 * SIZE(X), %xmm6

	movss	%xmm1, %xmm0
	SHUFPS_39 %xmm0, %xmm0
	mulps	ALPHA, %xmm0
	addps	-32 * SIZE(Y), %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	movss	%xmm2, %xmm1
	SHUFPS_39 %xmm1, %xmm1
	mulps	ALPHA, %xmm1
	addps	-28 * SIZE(Y), %xmm1
	movaps	%xmm1, -28 * SIZE(Y)

	movaps	 -5 * SIZE(X), %xmm7
	movaps	 -1 * SIZE(X), %xmm0

	movss	%xmm3, %xmm2
	SHUFPS_39 %xmm2, %xmm2
	mulps	ALPHA, %xmm2
	addps	-24 * SIZE(Y), %xmm2
	movaps	%xmm2, -24 * SIZE(Y)

	movss	%xmm4, %xmm3
	SHUFPS_39 %xmm3, %xmm3
	mulps	ALPHA, %xmm3
	addps	-20 * SIZE(Y), %xmm3
	movaps	%xmm3, -20 * SIZE(Y)

	movss	%xmm5, %xmm4
	SHUFPS_39 %xmm4, %xmm4
	mulps	ALPHA, %xmm4
	addps	-16 * SIZE(Y), %xmm4
	movaps	%xmm4, -16 * SIZE(Y)

	movss	%xmm6, %xmm5
	SHUFPS_39 %xmm5, %xmm5
	mulps	ALPHA, %xmm5
	addps	-12 * SIZE(Y), %xmm5
	movaps	%xmm5, -12 * SIZE(Y)

	movss	%xmm7, %xmm6
	SHUFPS_39 %xmm6, %xmm6
	mulps	ALPHA, %xmm6
	addps	-8 * SIZE(Y), %xmm6
	movaps	%xmm6,  -8 * SIZE(Y)

	movss	%xmm0, %xmm7
	SHUFPS_39 %xmm7, %xmm7
	mulps	ALPHA, %xmm7
	addps	-4 * SIZE(Y), %xmm7
	movaps	%xmm7,  -4 * SIZE(Y)

	subq	$-32 * SIZE, X
	subq	$-32 * SIZE, Y
	ALIGN_3

.L33:
	movq	M,  %rax
	andq	$16, %rax
	jle	.L34
	ALIGN_3

	movaps	-29 * SIZE(X), %xmm1
	movaps	-25 * SIZE(X), %xmm2
	movaps	-21 * SIZE(X), %xmm3
	movaps	-17 * SIZE(X), %xmm4

	movss	%xmm1, %xmm0
	SHUFPS_39 %xmm0, %xmm0
	mulps	ALPHA, %xmm0
	addps	-32 * SIZE(Y), %xmm0

	movss	%xmm2, %xmm1
	SHUFPS_39 %xmm1, %xmm1
	mulps	ALPHA, %xmm1
	addps	-28 * SIZE(Y), %xmm1

	movss	%xmm3, %xmm2
	SHUFPS_39 %xmm2, %xmm2
	mulps	ALPHA, %xmm2
	addps	-24 * SIZE(Y), %xmm2

	movss	%xmm4, %xmm3
	SHUFPS_39 %xmm3, %xmm3
	mulps	ALPHA, %xmm3
	addps	-20 * SIZE(Y), %xmm3

	movaps	%xmm0, -32 * SIZE(Y)
	movaps	%xmm1, -28 * SIZE(Y)
	movaps	%xmm2, -24 * SIZE(Y)
	movaps	%xmm3, -20 * SIZE(Y)

	movaps	%xmm4, %xmm0

	addq	$16 * SIZE, X
	addq	$16 * SIZE, Y
	ALIGN_3

.L34:
	movq	M,  %rax
	andq	$8, %rax
	jle	.L35
	ALIGN_3

	movaps	-29 * SIZE(X), %xmm1
	movaps	-25 * SIZE(X), %xmm2

	movss	%xmm1, %xmm0
	SHUFPS_39 %xmm0, %xmm0
	mulps	ALPHA, %xmm0
	addps	-32 * SIZE(Y), %xmm0

	movss	%xmm2, %xmm1
	SHUFPS_39 %xmm1, %xmm1
	mulps	ALPHA, %xmm1
	addps	-28 * SIZE(Y), %xmm1

	movaps	%xmm0, -32 * SIZE(Y)
	movaps	%xmm1, -28 * SIZE(Y)
	movaps	%xmm2, %xmm0

	addq	$8 * SIZE, X
	addq	$8 * SIZE, Y
	ALIGN_3

.L35:
	movq	M,  %rax
	andq	$4, %rax
	jle	.L36
	ALIGN_3

	movaps	-29 * SIZE(X), %xmm1

	movss	%xmm1, %xmm0
	SHUFPS_39 %xmm0, %xmm0
	mulps	ALPHA, %xmm0

	addps	-32 * SIZE(Y), %xmm0

	movaps	%xmm0, -32 * SIZE(Y)

	addq	$4 * SIZE, X
	addq	$4 * SIZE, Y
	ALIGN_3

.L36:
	movq	M,  %rax
	andq	$2, %rax
	jle	.L37
	ALIGN_3

	movsd	-32 * SIZE(X), %xmm0
	movsd	-32 * SIZE(Y), %xmm4

	mulps	ALPHA, %xmm0
	addps	%xmm4, %xmm0

	movsd	%xmm0, -32 * SIZE(Y)

	addq	$2 * SIZE, X
	addq	$2 * SIZE, Y
	ALIGN_3

.L37:
	movq	M,  %rax
	andq	$1, %rax
	jle	.L39
	ALIGN_3

	movss	-32 * SIZE(X), %xmm0
	mulss	ALPHA, %xmm0
	addss	-32 * SIZE(Y), %xmm0

	movss	%xmm0, 	-32 * SIZE(Y)
	addq	$SIZE, Y
	ALIGN_3

.L39:
	xorq	%rax,%rax

	RESTOREREGISTERS

	ret
	ALIGN_3

.L40:
	movaps	-35 * SIZE(X), %xmm0

	movq	M,  %rax
	sarq	$5, %rax
	jle	.L43

	movaps	-31 * SIZE(X), %xmm1
	movaps	-27 * SIZE(X), %xmm2
	movaps	-23 * SIZE(X), %xmm3
	movaps	-19 * SIZE(X), %xmm4

	decq	%rax
	jle .L42
	ALIGN_4

.L41:
	movaps	-15 * SIZE(X), %xmm5
	movaps	-11 * SIZE(X), %xmm6

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	movss	%xmm1, %xmm0
	shufps	$0x93, %xmm1, %xmm0
	mulps	ALPHA, %xmm0
	addps	-32 * SIZE(Y), %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	movss	%xmm2, %xmm1
	shufps	$0x93, %xmm2, %xmm1
	mulps	ALPHA, %xmm1
	addps	-28 * SIZE(Y), %xmm1
	movaps	%xmm1, -28 * SIZE(Y)

	movaps	 -7 * SIZE(X), %xmm7
	movaps	 -3 * SIZE(X), %xmm0

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movss	%xmm3, %xmm2
	shufps	$0x93, %xmm3, %xmm2
	mulps	ALPHA, %xmm2
	addps	-24 * SIZE(Y), %xmm2
	movaps	%xmm2, -24 * SIZE(Y)

	movss	%xmm4, %xmm3
	shufps	$0x93, %xmm4, %xmm3
	mulps	ALPHA, %xmm3
	addps	-20 * SIZE(Y), %xmm3
	movaps	%xmm3, -20 * SIZE(Y)

	movaps	  1 * SIZE(X), %xmm1
	movaps	  5 * SIZE(X), %xmm2

#if defined(PREFETCHW) && !defined(FETCH128)
	PREFETCHW (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	movss	%xmm5, %xmm4
	shufps	$0x93, %xmm5, %xmm4
	mulps	ALPHA, %xmm4
	addps	-16 * SIZE(Y), %xmm4
	movaps	%xmm4, -16 * SIZE(Y)

	movss	%xmm6, %xmm5
	shufps	$0x93, %xmm6, %xmm5
	mulps	ALPHA, %xmm5
	addps	-12 * SIZE(Y), %xmm5
	movaps	%xmm5, -12 * SIZE(Y)

	movaps	  9 * SIZE(X), %xmm3
	movaps	 13 * SIZE(X), %xmm4

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	movss	%xmm7, %xmm6
	shufps	$0x93, %xmm7, %xmm6
	mulps	ALPHA, %xmm6
	addps	 -8 * SIZE(Y), %xmm6
	movaps	%xmm6, -8 * SIZE(Y)

	movss	%xmm0, %xmm7
	shufps	$0x93, %xmm0, %xmm7
	mulps	ALPHA, %xmm7
	addps	 -4 * SIZE(Y), %xmm7
	movaps	%xmm7, -4 * SIZE(Y)

	subq	$-32 * SIZE, X
	subq	$-32 * SIZE, Y
	decq	%rax
	jg	.L41
	ALIGN_3

.L42:
	movaps	-15 * SIZE(X), %xmm5
	movaps	-11 * SIZE(X), %xmm6

	movss	%xmm1, %xmm0
	shufps	$0x93, %xmm1, %xmm0
	mulps	ALPHA, %xmm0
	addps	-32 * SIZE(Y), %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	movss	%xmm2, %xmm1
	shufps	$0x93, %xmm2, %xmm1
	mulps	ALPHA, %xmm1
	addps	-28 * SIZE(Y), %xmm1
	movaps	%xmm1, -28 * SIZE(Y)

	movaps	 -7 * SIZE(X), %xmm7
	movaps	 -3 * SIZE(X), %xmm0

	movss	%xmm3, %xmm2
	shufps	$0x93, %xmm3, %xmm2
	mulps	ALPHA, %xmm2
	addps	-24 * SIZE(Y), %xmm2
	movaps	%xmm2, -24 * SIZE(Y)

	movss	%xmm4, %xmm3
	shufps	$0x93, %xmm4, %xmm3
	mulps	ALPHA, %xmm3
	addps	-20 * SIZE(Y), %xmm3
	movaps	%xmm3, -20 * SIZE(Y)

	movss	%xmm5, %xmm4
	shufps	$0x93, %xmm5, %xmm4
	mulps	ALPHA, %xmm4
	addps	-16 * SIZE(Y), %xmm4
	movaps	%xmm4, -16 * SIZE(Y)

	movss	%xmm6, %xmm5
	shufps	$0x93, %xmm6, %xmm5
	mulps	ALPHA, %xmm5
	addps	-12 * SIZE(Y), %xmm5
	movaps	%xmm5, -12 * SIZE(Y)

	movss	%xmm7, %xmm6
	shufps	$0x93, %xmm7, %xmm6
	mulps	ALPHA, %xmm6
	addps	 -8 * SIZE(Y), %xmm6
	movaps	%xmm6, -8 * SIZE(Y)

	movss	%xmm0, %xmm7
	shufps	$0x93, %xmm0, %xmm7
	mulps	ALPHA, %xmm7
	addps	 -4 * SIZE(Y), %xmm7
	movaps	%xmm7, -4 * SIZE(Y)

	subq	$-32 * SIZE, X
	subq	$-32 * SIZE, Y
	ALIGN_3

.L43:
	movq	M,  %rax
	andq	$16, %rax
	jle	.L44
	ALIGN_3

	movaps	-31 * SIZE(X), %xmm1
	movaps	-27 * SIZE(X), %xmm2
	movaps	-23 * SIZE(X), %xmm3
	movaps	-19 * SIZE(X), %xmm4

	movss	%xmm1, %xmm0
	shufps	$0x93, %xmm1, %xmm0
	mulps	ALPHA, %xmm0
	addps	-32 * SIZE(Y), %xmm0
	movss	%xmm2, %xmm1
	shufps	$0x93, %xmm2, %xmm1
	mulps	ALPHA, %xmm1
	addps	-28 * SIZE(Y), %xmm1

	movss	%xmm3, %xmm2
	shufps	$0x93, %xmm3, %xmm2
	mulps	ALPHA, %xmm2
	addps	-24 * SIZE(Y), %xmm2
	movss	%xmm4, %xmm3
	shufps	$0x93, %xmm4, %xmm3
	mulps	ALPHA, %xmm3
	addps	-20 * SIZE(Y), %xmm3

	movaps	%xmm0, -32 * SIZE(Y)
	movaps	%xmm1, -28 * SIZE(Y)
	movaps	%xmm2, -24 * SIZE(Y)
	movaps	%xmm3, -20 * SIZE(Y)

	movaps	%xmm4, %xmm0

	addq	$16 * SIZE, X
	addq	$16 * SIZE, Y
	ALIGN_3

.L44:
	movq	M,  %rax
	andq	$8, %rax
	jle	.L45
	ALIGN_3

	movaps	-31 * SIZE(X), %xmm1
	movaps	-27 * SIZE(X), %xmm2

	movss	%xmm1, %xmm0
	shufps	$0x93, %xmm1, %xmm0
	mulps	ALPHA, %xmm0
	addps	-32 * SIZE(Y), %xmm0
	movss	%xmm2, %xmm1
	shufps	$0x93, %xmm2, %xmm1
	mulps	ALPHA, %xmm1
	addps	-28 * SIZE(Y), %xmm1

	movaps	%xmm0, -32 * SIZE(Y)
	movaps	%xmm1, -28 * SIZE(Y)
	movaps	%xmm2, %xmm0

	addq	$8 * SIZE, X
	addq	$8 * SIZE, Y
	ALIGN_3

.L45:
	movq	M,  %rax
	andq	$4, %rax
	jle	.L46
	ALIGN_3

	movaps	-31 * SIZE(X), %xmm1

	movss	%xmm1, %xmm0
	shufps	$0x93, %xmm1, %xmm0
	mulps	ALPHA, %xmm0

	addps	-32 * SIZE(Y), %xmm0

	movaps	%xmm0, -32 * SIZE(Y)

	addq	$4 * SIZE, X
	addq	$4 * SIZE, Y
	ALIGN_3

.L46:
	movq	M,  %rax
	andq	$2, %rax
	jle	.L47
	ALIGN_3

	movsd	-32 * SIZE(X), %xmm0
	movsd	-32 * SIZE(Y), %xmm4

	mulps	ALPHA, %xmm0
	addps	%xmm4, %xmm0

	movsd	%xmm0, -32 * SIZE(Y)

	addq	$2 * SIZE, X
	addq	$2 * SIZE, Y
	ALIGN_3

.L47:
	movq	M,  %rax
	andq	$1, %rax
	jle	.L49
	ALIGN_3

	movss	-32 * SIZE(X), %xmm0
	mulss	ALPHA, %xmm0
	addss	-32 * SIZE(Y), %xmm0

	movss	%xmm0, 	-32 * SIZE(Y)
	addq	$SIZE, Y
	ALIGN_3

.L49:
	xorq	%rax,%rax

	RESTOREREGISTERS

	ret

#else

	movq	M,  %rax
	sarq	$5, %rax
	jle	.L23

	movsd	-32 * SIZE(X), %xmm0
	movhps	-30 * SIZE(X), %xmm0
	movsd	-28 * SIZE(X), %xmm1
	movhps	-26 * SIZE(X), %xmm1
	movsd	-24 * SIZE(X), %xmm2
	movhps	-22 * SIZE(X), %xmm2
	movsd	-20 * SIZE(X), %xmm3
	movhps	-18 * SIZE(X), %xmm3

	decq	%rax
	jle .L22
	ALIGN_4

.L21:
	movsd	-16 * SIZE(X), %xmm4
	movhps	-14 * SIZE(X), %xmm4
	movsd	-12 * SIZE(X), %xmm5
	movhps	-10 * SIZE(X), %xmm5

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	mulps	ALPHA, %xmm0
	addps	-32 * SIZE(Y), %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	mulps	ALPHA, %xmm1
	addps	-28 * SIZE(Y), %xmm1
	movaps	%xmm1, -28 * SIZE(Y)

	movsd	 -8 * SIZE(X), %xmm6
	movhps	 -6 * SIZE(X), %xmm6
	movsd	 -4 * SIZE(X), %xmm7
	movhps	 -2 * SIZE(X), %xmm7

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	mulps	ALPHA, %xmm2
	addps	-24 * SIZE(Y), %xmm2
	movaps	%xmm2, -24 * SIZE(Y)

	mulps	ALPHA, %xmm3
	addps	-20 * SIZE(Y), %xmm3
	movaps	%xmm3, -20 * SIZE(Y)

	movsd	  0 * SIZE(X), %xmm0
	movhps	  2 * SIZE(X), %xmm0
	movsd	  4 * SIZE(X), %xmm1
	movhps	  6 * SIZE(X), %xmm1

#if defined(PREFETCHW) && !defined(FETCH128)
	PREFETCHW (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	mulps	ALPHA, %xmm4
	addps	-16 * SIZE(Y), %xmm4
	movaps	%xmm4, -16 * SIZE(Y)

	mulps	ALPHA, %xmm5
	addps	-12 * SIZE(Y), %xmm5
	movaps	%xmm5, -12 * SIZE(Y)

	movsd	  8 * SIZE(X), %xmm2
	movhps	 10 * SIZE(X), %xmm2
	movsd	 12 * SIZE(X), %xmm3
	movhps	 14 * SIZE(X), %xmm3

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	mulps	ALPHA, %xmm6
	addps	 -8 * SIZE(Y), %xmm6
	movaps	%xmm6,  -8 * SIZE(Y)

	mulps	ALPHA, %xmm7
	addps	 -4 * SIZE(Y), %xmm7
	movaps	%xmm7,  -4 * SIZE(Y)

	subq	$-32 * SIZE, X
	subq	$-32 * SIZE, Y
	decq	%rax
	jg	.L21
	ALIGN_3

.L22:
	movsd	-16 * SIZE(X), %xmm4
	movhps	-14 * SIZE(X), %xmm4
	movsd	-12 * SIZE(X), %xmm5
	movhps	-10 * SIZE(X), %xmm5

	mulps	ALPHA, %xmm0
	addps	-32 * SIZE(Y), %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	mulps	ALPHA, %xmm1
	addps	-28 * SIZE(Y), %xmm1
	movaps	%xmm1, -28 * SIZE(Y)

	movsd	 -8 * SIZE(X), %xmm6
	movhps	 -6 * SIZE(X), %xmm6
	movsd	 -4 * SIZE(X), %xmm7
	movhps	 -2 * SIZE(X), %xmm7

	mulps	ALPHA, %xmm2
	addps	-24 * SIZE(Y), %xmm2
	movaps	%xmm2, -24 * SIZE(Y)

	mulps	ALPHA, %xmm3
	addps	-20 * SIZE(Y), %xmm3
	movaps	%xmm3, -20 * SIZE(Y)

	mulps	ALPHA, %xmm4
	addps	-16 * SIZE(Y), %xmm4
	movaps	%xmm4, -16 * SIZE(Y)

	mulps	ALPHA, %xmm5
	addps	-12 * SIZE(Y), %xmm5
	movaps	%xmm5, -12 * SIZE(Y)

	mulps	ALPHA, %xmm6
	addps	 -8 * SIZE(Y), %xmm6
	movaps	%xmm6,  -8 * SIZE(Y)

	mulps	ALPHA, %xmm7
	addps	 -4 * SIZE(Y), %xmm7
	movaps	%xmm7,  -4 * SIZE(Y)

	subq	$-32 * SIZE, X
	subq	$-32 * SIZE, Y
	ALIGN_3

.L23:
	movq	M,  %rax
	andq	$16, %rax
	jle	.L24
	ALIGN_3

	movsd	-32 * SIZE(X), %xmm0
	movhps	-30 * SIZE(X), %xmm0
	movsd	-28 * SIZE(X), %xmm1
	movhps	-26 * SIZE(X), %xmm1

	mulps	ALPHA, %xmm0
	addps	-32 * SIZE(Y), %xmm0
	movaps	%xmm0, -32 * SIZE(Y)
	mulps	ALPHA, %xmm1
	addps	-28 * SIZE(Y), %xmm1
	movaps	%xmm1, -28 * SIZE(Y)

	movsd	-24 * SIZE(X), %xmm2
	movhps	-22 * SIZE(X), %xmm2
	movsd	-20 * SIZE(X), %xmm3
	movhps	-18 * SIZE(X), %xmm3

	mulps	ALPHA, %xmm2
	addps	-24 * SIZE(Y), %xmm2
	movaps	%xmm2, -24 * SIZE(Y)
	mulps	ALPHA, %xmm3
	addps	-20 * SIZE(Y), %xmm3
	movaps	%xmm3, -20 * SIZE(Y)

	addq	$16 * SIZE, X
	addq	$16 * SIZE, Y
	ALIGN_3

.L24:
	movq	M,  %rax
	andq	$8, %rax
	jle	.L25
	ALIGN_3

	movsd	-32 * SIZE(X), %xmm0
	movhps	-30 * SIZE(X), %xmm0
	movsd	-28 * SIZE(X), %xmm1
	movhps	-26 * SIZE(X), %xmm1

	mulps	ALPHA, %xmm0
	addps	-32 * SIZE(Y), %xmm0
	mulps	ALPHA, %xmm1
	addps	-28 * SIZE(Y), %xmm1

	movaps	%xmm0, -32 * SIZE(Y)
	movaps	%xmm1, -28 * SIZE(Y)

	addq	$8 * SIZE, X
	addq	$8 * SIZE, Y
	ALIGN_3

.L25:
	movq	M,  %rax
	andq	$4, %rax
	jle	.L26
	ALIGN_3

	movsd	-32 * SIZE(X), %xmm0
	movhps	-30 * SIZE(X), %xmm0

	mulps	ALPHA, %xmm0

	addps	-32 * SIZE(Y), %xmm0

	movaps	%xmm0, -32 * SIZE(Y)

	addq	$4 * SIZE, X
	addq	$4 * SIZE, Y
	ALIGN_3

.L26:
	movq	M,  %rax
	andq	$2, %rax
	jle	.L27
	ALIGN_3

	movsd	-32 * SIZE(X), %xmm0
	movsd	-32 * SIZE(Y), %xmm4

	mulps	ALPHA, %xmm0
	addps	%xmm4, %xmm0

	movsd	%xmm0, -32 * SIZE(Y)

	addq	$2 * SIZE, X
	addq	$2 * SIZE, Y
	ALIGN_3

.L27:
	movq	M,  %rax
	andq	$1, %rax
	jle	.L29
	ALIGN_3

	movss	-32 * SIZE(X), %xmm0
	mulss	ALPHA, %xmm0
	addss	-32 * SIZE(Y), %xmm0

	movss	%xmm0, -32 * SIZE(Y)
	addq	$SIZE, Y
	ALIGN_3

.L29:
	xorq	%rax,%rax

	RESTOREREGISTERS

	ret
#endif
	ALIGN_3


.L50:
	movq	M, %rax
	movq	Y, YY
//If incx==0 || incy==0, avoid unloop.
	cmpq	$0, INCX
	je  .L56
	cmpq	$0, INCY
	je  .L56

	sarq	$3,   %rax
	jle	.L55
	ALIGN_3

.L51:
	movss	(X), %xmm0
	addq	INCX, X
	mulss	ALPHA, %xmm0
	movss	(YY), %xmm6
	addq	INCY, YY
	addss	%xmm6, %xmm0

	movss	(X), %xmm1
	addq	INCX, X
	mulss	ALPHA, %xmm1
	movss	(YY), %xmm6
	addq	INCY, YY
	addss	%xmm6, %xmm1

	movss	(X), %xmm2
	addq	INCX, X
	mulss	ALPHA, %xmm2
	movss	(YY), %xmm6
	addq	INCY, YY
	addss	%xmm6, %xmm2

	movss	(X), %xmm3
	addq	INCX, X
	mulss	ALPHA, %xmm3
	movss	(YY), %xmm6
	addq	INCY, YY
	addss	%xmm6, %xmm3

	movss	%xmm0, (Y)
	addq	INCY, Y
	movss	%xmm1, (Y)
	addq	INCY, Y
	movss	%xmm2, (Y)
	addq	INCY, Y
	movss	%xmm3, (Y)
	addq	INCY, Y

	movss	(X), %xmm0
	addq	INCX, X
	mulss	ALPHA, %xmm0
	movss	(YY), %xmm6
	addq	INCY, YY
	addss	%xmm6, %xmm0

	movss	(X), %xmm1
	addq	INCX, X
	mulss	ALPHA, %xmm1
	movss	(YY), %xmm6
	addq	INCY, YY
	addss	%xmm6, %xmm1

	movss	(X), %xmm2
	addq	INCX, X
	mulss	ALPHA, %xmm2
	movss	(YY), %xmm6
	addq	INCY, YY
	addss	%xmm6, %xmm2

	movss	(X), %xmm3
	addq	INCX, X
	mulss	ALPHA, %xmm3
	movss	(YY), %xmm6
	addq	INCY, YY
	addss	%xmm6, %xmm3

	movss	%xmm0, (Y)
	addq	INCY, Y
	movss	%xmm1, (Y)
	addq	INCY, Y
	movss	%xmm2, (Y)
	addq	INCY, Y
	movss	%xmm3, (Y)
	addq	INCY, Y

	decq	%rax
	jg	.L51
	ALIGN_3

.L55:
	movq	M, %rax
	andq	$7,   %rax
	jle	.L59
	ALIGN_3

.L56:
	movss	(X), %xmm0
	addq	INCX, X
	mulss	ALPHA, %xmm0
	movss	(Y), %xmm6
	addss	%xmm6, %xmm0
	movss	%xmm0, (Y)
	addq	INCY, Y
	decq	%rax
	jg	.L56
	ALIGN_3

.L59:
	xorq	%rax,%rax

	RESTOREREGISTERS

	ret
	ALIGN_3


	EPILOGUE
