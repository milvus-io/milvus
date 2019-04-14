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

	unpcklpd ALPHA, ALPHA

	leaq	(, INCX, SIZE), INCX
	leaq	(, INCY, SIZE), INCY

	testq	M, M
	jle	.L47

	cmpq	$SIZE, INCX
	jne	.L40
	cmpq	$SIZE, INCY
	jne	.L40

	testq	$SIZE, Y
	je	.L10

	movsd	(X), %xmm0
	mulsd	ALPHA, %xmm0
	addsd	(Y), %xmm0
	movsd	%xmm0, (Y)
	addq	$1 * SIZE, X
	addq	$1 * SIZE, Y
	decq	M
	jle	.L19
	ALIGN_4

.L10:
	subq	$-16 * SIZE, X
	subq	$-16 * SIZE, Y

	testq	$SIZE, X
	jne	.L20

	movq	M,  %rax
	sarq	$4, %rax
	jle	.L13

	movaps	-16 * SIZE(X), %xmm0
	movaps	-14 * SIZE(X), %xmm1
	movaps	-12 * SIZE(X), %xmm2
	movaps	-10 * SIZE(X), %xmm3

	decq	%rax
	jle .L12
	ALIGN_3

.L11:
	movaps	 -8 * SIZE(X), %xmm4
	movaps	 -6 * SIZE(X), %xmm5

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	mulpd	ALPHA, %xmm0
	addpd	-16 * SIZE(Y), %xmm0
	movaps	%xmm0, -16 * SIZE(Y)

	mulpd	ALPHA, %xmm1
	addpd	-14 * SIZE(Y), %xmm1
	movaps	%xmm1, -14 * SIZE(Y)

	movaps	 -4 * SIZE(X), %xmm6
	movaps	 -2 * SIZE(X), %xmm7

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	mulpd	ALPHA, %xmm2
	addpd	-12 * SIZE(Y), %xmm2
	movaps	%xmm2, -12 * SIZE(Y)

	mulpd	ALPHA, %xmm3
	addpd	-10 * SIZE(Y), %xmm3
	movaps	%xmm3, -10 * SIZE(Y)

	movaps	 0 * SIZE(X), %xmm0
	movaps	 2 * SIZE(X), %xmm1

#if defined(PREFETCHW) && !defined(FETCH128)
	PREFETCHW (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	mulpd	ALPHA, %xmm4
	addpd	 -8 * SIZE(Y), %xmm4
	movaps	%xmm4, -8 * SIZE(Y)

	mulpd	ALPHA, %xmm5
	addpd	 -6 * SIZE(Y), %xmm5
	movaps	%xmm5, -6 * SIZE(Y)

	movaps	 4 * SIZE(X), %xmm2
	movaps	 6 * SIZE(X), %xmm3

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	mulpd	ALPHA, %xmm6
	addpd	 -4 * SIZE(Y), %xmm6
	movaps	%xmm6, -4 * SIZE(Y)

	mulpd	ALPHA, %xmm7
	addpd	 -2 * SIZE(Y), %xmm7
	movaps	%xmm7, -2 * SIZE(Y)

	subq	$-16 * SIZE, Y
	subq	$-16 * SIZE, X
	decq	%rax
	jg	.L11
	ALIGN_3

.L12:
	movaps	 -8 * SIZE(X), %xmm4
	movaps	 -6 * SIZE(X), %xmm5

	mulpd	ALPHA, %xmm0
	addpd	-16 * SIZE(Y), %xmm0
	movaps	%xmm0, -16 * SIZE(Y)

	mulpd	ALPHA, %xmm1
	addpd	-14 * SIZE(Y), %xmm1
	movaps	%xmm1, -14 * SIZE(Y)

	movaps	 -4 * SIZE(X), %xmm6
	movaps	 -2 * SIZE(X), %xmm7

	mulpd	ALPHA, %xmm2
	addpd	-12 * SIZE(Y), %xmm2
	movaps	%xmm2, -12 * SIZE(Y)

	mulpd	ALPHA, %xmm3
	addpd	-10 * SIZE(Y), %xmm3
	movaps	%xmm3, -10 * SIZE(Y)

	mulpd	ALPHA, %xmm4
	addpd	 -8 * SIZE(Y), %xmm4
	movaps	%xmm4,  -8 * SIZE(Y)

	mulpd	ALPHA, %xmm5
	addpd	 -6 * SIZE(Y), %xmm5
	movaps	%xmm5,  -6 * SIZE(Y)

	mulpd	ALPHA, %xmm6
	addpd	 -4 * SIZE(Y), %xmm6
	movaps	%xmm6,  -4 * SIZE(Y)

	mulpd	ALPHA, %xmm7
	addpd	 -2 * SIZE(Y), %xmm7
	movaps	%xmm7,  -2 * SIZE(Y)

	subq	$-16 * SIZE, Y
	subq	$-16 * SIZE, X
	ALIGN_3

.L13:
	movq	M,  %rax
	andq	$8, %rax
	jle	.L14
	ALIGN_3

	movaps	-16 * SIZE(X), %xmm0
	movaps	-14 * SIZE(X), %xmm1
	movaps	-12 * SIZE(X), %xmm2
	movaps	-10 * SIZE(X), %xmm3

	mulpd	ALPHA, %xmm0
	addpd	-16 * SIZE(Y), %xmm0
	mulpd	ALPHA, %xmm1
	addpd	-14 * SIZE(Y), %xmm1
	mulpd	ALPHA, %xmm2
	addpd	-12 * SIZE(Y), %xmm2
	mulpd	ALPHA, %xmm3
	addpd	-10 * SIZE(Y), %xmm3

	movaps	%xmm0, -16 * SIZE(Y)
	movaps	%xmm1, -14 * SIZE(Y)
	movaps	%xmm2, -12 * SIZE(Y)
	movaps	%xmm3, -10 * SIZE(Y)

	addq	$8 * SIZE, X
	addq	$8 * SIZE, Y
	ALIGN_3

.L14:
	movq	M,  %rax
	andq	$4, %rax
	jle	.L15
	ALIGN_3

	movaps	-16 * SIZE(X), %xmm0
	movaps	-14 * SIZE(X), %xmm1

	mulpd	ALPHA, %xmm0
	mulpd	ALPHA, %xmm1

	addpd	-16 * SIZE(Y), %xmm0
	addpd	-14 * SIZE(Y), %xmm1

	movaps	%xmm0, -16 * SIZE(Y)
	movaps	%xmm1, -14 * SIZE(Y)

	addq	$4 * SIZE, X
	addq	$4 * SIZE, Y
	ALIGN_3

.L15:
	movq	M,  %rax
	andq	$2, %rax
	jle	.L16
	ALIGN_3

	movaps	-16 * SIZE(X), %xmm0
	mulpd	ALPHA, %xmm0
	addpd	-16 * SIZE(Y), %xmm0
	movaps	%xmm0, -16 * SIZE(Y)

	addq	$2 * SIZE, X
	addq	$2 * SIZE, Y
	ALIGN_3

.L16:
	movq	M,  %rax
	andq	$1, %rax
	jle	.L19
	ALIGN_3

	movsd	-16 * SIZE(X), %xmm0
	mulsd	ALPHA, %xmm0
	addsd	-16 * SIZE(Y), %xmm0

	movsd	%xmm0, 	-16 * SIZE(Y)
	ALIGN_3

.L19:
	xorq	%rax,%rax

	RESTOREREGISTERS

	ret
	ALIGN_3

.L20:
#ifdef ALIGNED_ACCESS

	movhps	-16 * SIZE(X), %xmm0

	movq	M,  %rax
	sarq	$4, %rax
	jle	.L23

	movaps	-15 * SIZE(X), %xmm1
	movaps	-13 * SIZE(X), %xmm2
	movaps	-11 * SIZE(X), %xmm3

	decq	%rax
	jle .L22
	ALIGN_4

.L21:
	movaps	 -9 * SIZE(X), %xmm4
	movaps	 -7 * SIZE(X), %xmm5

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	SHUFPD_1 %xmm1, %xmm0
	mulpd	ALPHA, %xmm0
	addpd	-16 * SIZE(Y), %xmm0
	movaps	%xmm0, -16 * SIZE(Y)

	SHUFPD_1 %xmm2, %xmm1
	mulpd	ALPHA, %xmm1
	addpd	-14 * SIZE(Y), %xmm1
	movaps	%xmm1, -14 * SIZE(Y)

	movaps	 -5 * SIZE(X), %xmm6
	movaps	 -3 * SIZE(X), %xmm7

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	SHUFPD_1 %xmm3, %xmm2
	mulpd	ALPHA, %xmm2
	addpd	-12 * SIZE(Y), %xmm2
	movaps	%xmm2, -12 * SIZE(Y)

	SHUFPD_1 %xmm4, %xmm3
	mulpd	ALPHA, %xmm3
	addpd	-10 * SIZE(Y), %xmm3
	movaps	%xmm3, -10 * SIZE(Y)

	movaps	-1 * SIZE(X), %xmm0
	movaps	 1 * SIZE(X), %xmm1

#if defined(PREFETCHW) && !defined(FETCH128)
	PREFETCHW (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	SHUFPD_1 %xmm5, %xmm4
	mulpd	ALPHA, %xmm4
	addpd	 -8 * SIZE(Y), %xmm4
	movaps	%xmm4,  -8 * SIZE(Y)

	SHUFPD_1 %xmm6, %xmm5
	mulpd	ALPHA, %xmm5
	addpd	 -6 * SIZE(Y), %xmm5
	movaps	%xmm5, -6 * SIZE(Y)

	movaps	 3 * SIZE(X), %xmm2
	movaps	 5 * SIZE(X), %xmm3

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	SHUFPD_1 %xmm7, %xmm6
	mulpd	ALPHA, %xmm6
	addpd	 -4 * SIZE(Y), %xmm6
	movaps	%xmm6, -4 * SIZE(Y)

	SHUFPD_1 %xmm0, %xmm7
	mulpd	ALPHA, %xmm7
	addpd	-2 * SIZE(Y), %xmm7
	movaps	%xmm7, -2 * SIZE(Y)

	subq	$-16 * SIZE, X
	subq	$-16 * SIZE, Y
	decq	%rax
	jg	.L21
	ALIGN_3

.L22:
	movaps	 -9 * SIZE(X), %xmm4
	movaps	 -7 * SIZE(X), %xmm5

	SHUFPD_1 %xmm1, %xmm0
	mulpd	ALPHA, %xmm0
	addpd	-16 * SIZE(Y), %xmm0
	movaps	%xmm0, -16 * SIZE(Y)
	movaps	-1 * SIZE(X), %xmm0

	SHUFPD_1 %xmm2, %xmm1
	mulpd	ALPHA, %xmm1
	addpd	-14 * SIZE(Y), %xmm1
	movaps	%xmm1, -14 * SIZE(Y)

	movaps	 -5 * SIZE(X), %xmm6
	movaps	 -3 * SIZE(X), %xmm7

	SHUFPD_1 %xmm3, %xmm2
	mulpd	ALPHA, %xmm2
	addpd	-12 * SIZE(Y), %xmm2
	movaps	%xmm2, -12 * SIZE(Y)

	SHUFPD_1 %xmm4, %xmm3
	mulpd	ALPHA, %xmm3
	addpd	-10 * SIZE(Y), %xmm3
	movaps	%xmm3, -10 * SIZE(Y)

	SHUFPD_1 %xmm5, %xmm4
	mulpd	ALPHA, %xmm4
	addpd	 -8 * SIZE(Y), %xmm4
	movaps	%xmm4,  -8 * SIZE(Y)

	SHUFPD_1 %xmm6, %xmm5
	mulpd	ALPHA, %xmm5
	addpd	 -6 * SIZE(Y), %xmm5
	movaps	%xmm5,  -6 * SIZE(Y)

	SHUFPD_1 %xmm7, %xmm6
	mulpd	ALPHA, %xmm6
	addpd	 -4 * SIZE(Y), %xmm6
	movaps	%xmm6,  -4 * SIZE(Y)

	SHUFPD_1 %xmm0, %xmm7
	mulpd	ALPHA, %xmm7
	addpd	 -2 * SIZE(Y), %xmm7
	movaps	%xmm7,  -2 * SIZE(Y)

	subq	$-16 * SIZE, X
	subq	$-16 * SIZE, Y
	ALIGN_3

.L23:
	movq	M,  %rax
	andq	$8, %rax
	jle	.L24
	ALIGN_3

	movaps	-15 * SIZE(X), %xmm1
	movaps	-13 * SIZE(X), %xmm2
	movaps	-11 * SIZE(X), %xmm3
	movaps	 -9 * SIZE(X), %xmm8

	SHUFPD_1 %xmm1, %xmm0
	mulpd	ALPHA, %xmm0
	addpd	-16 * SIZE(Y), %xmm0
	movaps	%xmm0, -16 * SIZE(Y)

	SHUFPD_1 %xmm2, %xmm1
	mulpd	ALPHA, %xmm1
	addpd	-14 * SIZE(Y), %xmm1
	movaps	%xmm1, -14 * SIZE(Y)

	SHUFPD_1 %xmm3, %xmm2
	mulpd	ALPHA, %xmm2
	addpd	-12 * SIZE(Y), %xmm2
	movaps	%xmm2, -12 * SIZE(Y)

	SHUFPD_1 %xmm8, %xmm3
	mulpd	ALPHA, %xmm3
	addpd	-10 * SIZE(Y), %xmm3
	movaps	%xmm3, -10 * SIZE(Y)

	movaps	%xmm8, %xmm0

	addq	$8 * SIZE, X
	addq	$8 * SIZE, Y
	ALIGN_3

.L24:
	movq	M,  %rax
	andq	$4, %rax
	jle	.L25
	ALIGN_3

	movaps	-15 * SIZE(X), %xmm1
	movaps	-13 * SIZE(X), %xmm2

	SHUFPD_1 %xmm1, %xmm0
	SHUFPD_1 %xmm2, %xmm1

	mulpd	ALPHA, %xmm0
	mulpd	ALPHA, %xmm1

	addpd	-16 * SIZE(Y), %xmm0
	addpd	-14 * SIZE(Y), %xmm1

	movaps	%xmm0, -16 * SIZE(Y)
	movaps	%xmm1, -14 * SIZE(Y)
	movaps	%xmm2, %xmm0

	addq	$4 * SIZE, X
	addq	$4 * SIZE, Y
	ALIGN_3

.L25:
	movq	M,  %rax
	andq	$2, %rax
	jle	.L26
	ALIGN_3

	movaps	-15 * SIZE(X), %xmm1
	SHUFPD_1 %xmm1, %xmm0
	mulpd	ALPHA,  %xmm0
	addpd	-16 * SIZE(Y), %xmm0

	movaps	%xmm0, -16 * SIZE(Y)

	addq	$2 * SIZE, X
	addq	$2 * SIZE, Y
	ALIGN_3

.L26:
	movq	M,  %rax
	andq	$1, %rax
	jle	.L29
	ALIGN_3

	movsd	-16 * SIZE(X), %xmm0
	mulsd	ALPHA, %xmm0
	addsd	-16 * SIZE(Y), %xmm0

	movsd	%xmm0, 	-16 * SIZE(Y)
	ALIGN_3

.L29:
	xorq	%rax,%rax

	RESTOREREGISTERS

	ret
	ALIGN_3

#else
	movq	M,  %rax
	sarq	$4, %rax
	jle	.L23

	movsd	-16 * SIZE(X), %xmm0
	movhps	-15 * SIZE(X), %xmm0
	movsd	-14 * SIZE(X), %xmm1
	movhps	-13 * SIZE(X), %xmm1
	movsd	-12 * SIZE(X), %xmm2
	movhps	-11 * SIZE(X), %xmm2
	movsd	-10 * SIZE(X), %xmm3
	movhps	 -9 * SIZE(X), %xmm3

	decq	%rax
	jle .L22
	ALIGN_3

.L21:
	movsd	 -8 * SIZE(X), %xmm4
	movhps	 -7 * SIZE(X), %xmm4
	movsd	 -6 * SIZE(X), %xmm5
	movhps	 -5 * SIZE(X), %xmm5

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	mulpd	ALPHA, %xmm0
	addpd	-16 * SIZE(Y), %xmm0
	movaps	%xmm0, -16 * SIZE(Y)

	mulpd	ALPHA, %xmm1
	addpd	-14 * SIZE(Y), %xmm1
	movaps	%xmm1, -14 * SIZE(Y)

	movsd	 -4 * SIZE(X), %xmm6
	movhps	 -3 * SIZE(X), %xmm6
	movsd	 -2 * SIZE(X), %xmm7
	movhps	 -1 * SIZE(X), %xmm7

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	mulpd	ALPHA, %xmm2
	addpd	-12 * SIZE(Y), %xmm2
	movaps	%xmm2, -12 * SIZE(Y)

	mulpd	ALPHA, %xmm3
	addpd	-10 * SIZE(Y), %xmm3
	movaps	%xmm3, -10 * SIZE(Y)

	movsd	 0 * SIZE(X), %xmm0
	movhps	 1 * SIZE(X), %xmm0
	movsd	 2 * SIZE(X), %xmm1
	movhps	 3 * SIZE(X), %xmm1

#if defined(PREFETCHW) && !defined(FETCH128)
	PREFETCHW (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	mulpd	ALPHA, %xmm4
	addpd	 -8 * SIZE(Y), %xmm4
	movaps	%xmm4, -8 * SIZE(Y)

	mulpd	ALPHA, %xmm5
	addpd	 -6 * SIZE(Y), %xmm5
	movaps	%xmm5, -6 * SIZE(Y)

	movsd	 4 * SIZE(X), %xmm2
	movhps	 5 * SIZE(X), %xmm2
	movsd	 6 * SIZE(X), %xmm3
	movhps	 7 * SIZE(X), %xmm3

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	mulpd	ALPHA, %xmm6
	addpd	 -4 * SIZE(Y), %xmm6
	movaps	%xmm6, -4 * SIZE(Y)

	mulpd	ALPHA, %xmm7
	addpd	 -2 * SIZE(Y), %xmm7
	movaps	%xmm7, -2 * SIZE(Y)

	subq	$-16 * SIZE, Y
	subq	$-16 * SIZE, X
	decq	%rax
	jg	.L21
	ALIGN_3

.L22:
	movsd	 -8 * SIZE(X), %xmm4
	movhps	 -7 * SIZE(X), %xmm4
	movsd	 -6 * SIZE(X), %xmm5
	movhps	 -5 * SIZE(X), %xmm5

	mulpd	ALPHA, %xmm0
	addpd	-16 * SIZE(Y), %xmm0
	movaps	%xmm0, -16 * SIZE(Y)

	mulpd	ALPHA, %xmm1
	addpd	-14 * SIZE(Y), %xmm1
	movaps	%xmm1, -14 * SIZE(Y)

	movsd	 -4 * SIZE(X), %xmm6
	movhps	 -3 * SIZE(X), %xmm6
	movsd	 -2 * SIZE(X), %xmm7
	movhps	 -1 * SIZE(X), %xmm7

	mulpd	ALPHA, %xmm2
	addpd	-12 * SIZE(Y), %xmm2
	movaps	%xmm2, -12 * SIZE(Y)

	mulpd	ALPHA, %xmm3
	addpd	-10 * SIZE(Y), %xmm3
	movaps	%xmm3, -10 * SIZE(Y)

	mulpd	ALPHA, %xmm4
	addpd	 -8 * SIZE(Y), %xmm4
	movaps	%xmm4,  -8 * SIZE(Y)

	mulpd	ALPHA, %xmm5
	addpd	 -6 * SIZE(Y), %xmm5
	movaps	%xmm5,  -6 * SIZE(Y)

	mulpd	ALPHA, %xmm6
	addpd	 -4 * SIZE(Y), %xmm6
	movaps	%xmm6,  -4 * SIZE(Y)

	mulpd	ALPHA, %xmm7
	addpd	 -2 * SIZE(Y), %xmm7
	movaps	%xmm7,  -2 * SIZE(Y)

	subq	$-16 * SIZE, Y
	subq	$-16 * SIZE, X
	ALIGN_3

.L23:
	movq	M,  %rax
	andq	$8, %rax
	jle	.L24
	ALIGN_3

	movsd	-16 * SIZE(X), %xmm0
	movhps	-15 * SIZE(X), %xmm0
	movsd	-14 * SIZE(X), %xmm1
	movhps	-13 * SIZE(X), %xmm1
	movsd	-12 * SIZE(X), %xmm2
	movhps	-11 * SIZE(X), %xmm2
	movsd	-10 * SIZE(X), %xmm3
	movhps	 -9 * SIZE(X), %xmm3

	mulpd	ALPHA, %xmm0
	addpd	-16 * SIZE(Y), %xmm0
	mulpd	ALPHA, %xmm1
	addpd	-14 * SIZE(Y), %xmm1
	mulpd	ALPHA, %xmm2
	addpd	-12 * SIZE(Y), %xmm2
	mulpd	ALPHA, %xmm3
	addpd	-10 * SIZE(Y), %xmm3

	movaps	%xmm0, -16 * SIZE(Y)
	movaps	%xmm1, -14 * SIZE(Y)
	movaps	%xmm2, -12 * SIZE(Y)
	movaps	%xmm3, -10 * SIZE(Y)

	addq	$8 * SIZE, X
	addq	$8 * SIZE, Y
	ALIGN_3

.L24:
	movq	M,  %rax
	andq	$4, %rax
	jle	.L25
	ALIGN_3

	movsd	-16 * SIZE(X), %xmm0
	movhps	-15 * SIZE(X), %xmm0
	movsd	-14 * SIZE(X), %xmm1
	movhps	-13 * SIZE(X), %xmm1

	mulpd	ALPHA, %xmm0
	mulpd	ALPHA, %xmm1

	addpd	-16 * SIZE(Y), %xmm0
	addpd	-14 * SIZE(Y), %xmm1

	movaps	%xmm0, -16 * SIZE(Y)
	movaps	%xmm1, -14 * SIZE(Y)

	addq	$4 * SIZE, X
	addq	$4 * SIZE, Y
	ALIGN_3

.L25:
	movq	M,  %rax
	andq	$2, %rax
	jle	.L26
	ALIGN_3

	movsd	-16 * SIZE(X), %xmm0
	movhps	-15 * SIZE(X), %xmm0
	mulpd	ALPHA, %xmm0
	addpd	-16 * SIZE(Y), %xmm0
	movaps	%xmm0, -16 * SIZE(Y)

	addq	$2 * SIZE, X
	addq	$2 * SIZE, Y
	ALIGN_3

.L26:
	movq	M,  %rax
	andq	$1, %rax
	jle	.L29
	ALIGN_3

	movsd	-16 * SIZE(X), %xmm0
	mulsd	ALPHA, %xmm0
	addsd	-16 * SIZE(Y), %xmm0

	movsd	%xmm0, 	-16 * SIZE(Y)
	ALIGN_3

.L29:
	xorq	%rax,%rax

	RESTOREREGISTERS

	ret
	ALIGN_3
#endif

.L40:
	movq	Y, YY
	movq	M,  %rax
//If incx==0 || incy==0, avoid unloop.
	cmpq	$0, INCX
	je  .L46
	cmpq	$0, INCY
	je  .L46

	sarq	$3, %rax
	jle	.L45
	ALIGN_3

.L41:
	movsd	0 * SIZE(X), %xmm0
	addq	INCX, X
	movhpd	0 * SIZE(X), %xmm0
	addq	INCX, X
	mulpd	ALPHA, %xmm0

	movsd	0 * SIZE(YY), %xmm6
	addq	INCY, YY
	movhpd	0 * SIZE(YY), %xmm6
	addq	INCY, YY
	addpd	%xmm6, %xmm0

	movsd	0 * SIZE(X), %xmm1
	addq	INCX, X
	movhpd	0 * SIZE(X), %xmm1
	addq	INCX, X
	mulpd	ALPHA, %xmm1

	movsd	0 * SIZE(YY), %xmm6
	addq	INCY, YY
	movhpd	0 * SIZE(YY), %xmm6
	addq	INCY, YY
	addpd	%xmm6, %xmm1

	movsd	0 * SIZE(X), %xmm2
	addq	INCX, X
	movhpd	0 * SIZE(X), %xmm2
	addq	INCX, X
	mulpd	ALPHA, %xmm2

	movsd	0 * SIZE(YY), %xmm6
	addq	INCY, YY
	movhpd	0 * SIZE(YY), %xmm6
	addq	INCY, YY
	addpd	%xmm6, %xmm2

	movsd	0 * SIZE(X), %xmm3
	addq	INCX, X
	movhpd	0 * SIZE(X), %xmm3
	addq	INCX, X
	mulpd	ALPHA, %xmm3

	movsd	0 * SIZE(YY), %xmm6
	addq	INCY, YY
	movhpd	0 * SIZE(YY), %xmm6
	addq	INCY, YY
	addpd	%xmm6, %xmm3

	movsd	%xmm0, 0 * SIZE(Y)
	addq	INCY, Y
	movhpd	%xmm0, 0 * SIZE(Y)
	addq	INCY, Y
	movsd	%xmm1, 0 * SIZE(Y)
	addq	INCY, Y
	movhpd	%xmm1, 0 * SIZE(Y)
	addq	INCY, Y
	movsd	%xmm2, 0 * SIZE(Y)
	addq	INCY, Y
	movhpd	%xmm2, 0 * SIZE(Y)
	addq	INCY, Y
	movsd	%xmm3, 0 * SIZE(Y)
	addq	INCY, Y
	movhpd	%xmm3, 0 * SIZE(Y)
	addq	INCY, Y

	decq	%rax
	jg	.L41
	ALIGN_3

.L45:
	movq	M,  %rax
	andq	$7, %rax
	jle	.L47
	ALIGN_3

.L46:
	movsd	(X), %xmm0
	addq	INCX, X
	mulsd	%xmm15, %xmm0
	addsd	(Y), %xmm0
	movsd	%xmm0, (Y)
	addq	INCY, Y
	decq	%rax
	jg	.L46
	ALIGN_3

.L47:
	xorq	%rax, %rax

	RESTOREREGISTERS

	ret

	EPILOGUE
