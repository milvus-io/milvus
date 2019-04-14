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
#define STACK_ALPHA	16 + STACK + ARGS(%esp)
#define STACK_X		24 + STACK + ARGS(%esp)
#define STACK_INCX	28 + STACK + ARGS(%esp)
#define STACK_Y		32 + STACK + ARGS(%esp)
#define STACK_INCY	36 + STACK + ARGS(%esp)

#define M	 %ebx
#define X	 %esi
#define Y	 %edi
#define INCX	 %ecx
#define INCY	 %edx
#define YY	 %ebp

#define ALPHA	%xmm7

#include "l1param.h"

	PROLOGUE
	PROFCODE

	pushl	%edi
	pushl	%esi
	pushl	%ebx
	pushl	%ebp

	movl	STACK_M,    M
	movsd	STACK_ALPHA, ALPHA
	movl	STACK_X,    X
	movl	STACK_INCX, INCX
	movl	STACK_Y,    Y
	movl	STACK_INCY, INCY

	unpcklpd ALPHA, ALPHA

	leal	(, INCX, SIZE), INCX
	leal	(, INCY, SIZE), INCY

	testl	M, M
	jle	.L47

	cmpl	$SIZE, INCX
	jne	.L40
	cmpl	$SIZE, INCY
	jne	.L40

	testl	$SIZE, Y
	je	.L10

	movsd	(X), %xmm0
	mulsd	ALPHA, %xmm0
	addsd	(Y), %xmm0
	movsd	%xmm0, (Y)
	addl	$1 * SIZE, X
	addl	$1 * SIZE, Y
	decl	M
	jle	.L19
	ALIGN_4

.L10:
	subl	$-16 * SIZE, X
	subl	$-16 * SIZE, Y

	testl	$SIZE, X
	jne	.L20

	movl	M,  %eax
	sarl	$4, %eax
	jle	.L13

	movaps	-16 * SIZE(X), %xmm0
	movaps	-14 * SIZE(X), %xmm1
	movaps	-12 * SIZE(X), %xmm2
	movaps	-10 * SIZE(X), %xmm3

	decl	%eax
	jle .L12
	ALIGN_3

.L11:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	mulpd	ALPHA, %xmm0
	addpd	-16 * SIZE(Y), %xmm0
	movaps	%xmm0, -16 * SIZE(Y)
	movaps	 -8 * SIZE(X), %xmm0

	mulpd	ALPHA, %xmm1
	addpd	-14 * SIZE(Y), %xmm1
	movaps	%xmm1, -14 * SIZE(Y)
	movaps	 -6 * SIZE(X), %xmm1

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	mulpd	ALPHA, %xmm2
	addpd	-12 * SIZE(Y), %xmm2
	movaps	%xmm2, -12 * SIZE(Y)
	movaps	 -4 * SIZE(X), %xmm2

	mulpd	ALPHA, %xmm3
	addpd	-10 * SIZE(Y), %xmm3
	movaps	%xmm3, -10 * SIZE(Y)
	movaps	 -2 * SIZE(X), %xmm3

#if defined(PREFETCHW) && !defined(FETCH128)
	PREFETCHW (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	mulpd	ALPHA, %xmm0
	addpd	 -8 * SIZE(Y), %xmm0
	movaps	%xmm0,  -8 * SIZE(Y)
	movaps	  0 * SIZE(X), %xmm0

	mulpd	ALPHA, %xmm1
	addpd	 -6 * SIZE(Y), %xmm1
	movaps	%xmm1,  -6 * SIZE(Y)
	movaps	  2 * SIZE(X), %xmm1

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	mulpd	ALPHA, %xmm2
	addpd	 -4 * SIZE(Y), %xmm2
	movaps	%xmm2,  -4 * SIZE(Y)
	movaps	  4 * SIZE(X), %xmm2

	mulpd	ALPHA, %xmm3
	addpd	 -2 * SIZE(Y), %xmm3
	movaps	%xmm3,  -2 * SIZE(Y)
	movaps	  6 * SIZE(X), %xmm3

	subl	$-16 * SIZE, Y
	subl	$-16 * SIZE, X
	decl	%eax
	jg	.L11
	ALIGN_3

.L12:
	mulpd	ALPHA, %xmm0
	addpd	-16 * SIZE(Y), %xmm0
	movaps	%xmm0, -16 * SIZE(Y)
	movaps	 -8 * SIZE(X), %xmm0

	mulpd	ALPHA, %xmm1
	addpd	-14 * SIZE(Y), %xmm1
	movaps	%xmm1, -14 * SIZE(Y)
	movaps	 -6 * SIZE(X), %xmm1

	mulpd	ALPHA, %xmm2
	addpd	-12 * SIZE(Y), %xmm2
	movaps	%xmm2, -12 * SIZE(Y)
	movaps	 -4 * SIZE(X), %xmm2

	mulpd	ALPHA, %xmm3
	addpd	-10 * SIZE(Y), %xmm3
	movaps	%xmm3, -10 * SIZE(Y)
	movaps	 -2 * SIZE(X), %xmm3

	mulpd	ALPHA, %xmm0
	addpd	 -8 * SIZE(Y), %xmm0
	movaps	%xmm0,  -8 * SIZE(Y)

	mulpd	ALPHA, %xmm1
	addpd	 -6 * SIZE(Y), %xmm1
	movaps	%xmm1,  -6 * SIZE(Y)

	mulpd	ALPHA, %xmm2
	addpd	 -4 * SIZE(Y), %xmm2
	movaps	%xmm2,  -4 * SIZE(Y)

	mulpd	ALPHA, %xmm3
	addpd	 -2 * SIZE(Y), %xmm3
	movaps	%xmm3,  -2 * SIZE(Y)

	subl	$-16 * SIZE, Y
	subl	$-16 * SIZE, X
	ALIGN_3

.L13:
	movl	M,  %eax
	andl	$8, %eax
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

	addl	$8 * SIZE, X
	addl	$8 * SIZE, Y
	ALIGN_3

.L14:
	movl	M,  %eax
	andl	$4, %eax
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

	addl	$4 * SIZE, X
	addl	$4 * SIZE, Y
	ALIGN_3

.L15:
	movl	M,  %eax
	andl	$2, %eax
	jle	.L16
	ALIGN_3

	movaps	-16 * SIZE(X), %xmm0
	mulpd	ALPHA, %xmm0
	addpd	-16 * SIZE(Y), %xmm0
	movaps	%xmm0, -16 * SIZE(Y)

	addl	$2 * SIZE, X
	addl	$2 * SIZE, Y
	ALIGN_3

.L16:
	movl	M,  %eax
	andl	$1, %eax
	jle	.L19
	ALIGN_3

	movsd	-16 * SIZE(X), %xmm0
	mulsd	ALPHA, %xmm0
	addsd	-16 * SIZE(Y), %xmm0

	movsd	%xmm0, 	-16 * SIZE(Y)
	ALIGN_3

.L19:
	popl	%ebp
	popl	%ebx
	popl	%esi
	popl	%edi
	ret
	ALIGN_3

.L20:
#ifdef ALIGNED_ACCESS

	movhps	-16 * SIZE(X), %xmm0

	movl	M,  %eax
	sarl	$4, %eax
	jle	.L23

	movaps	-15 * SIZE(X), %xmm1
	movaps	-13 * SIZE(X), %xmm2
	movaps	-11 * SIZE(X), %xmm3

	decl	%eax
	jle .L22
	ALIGN_4

.L21:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	SHUFPD_1 %xmm1, %xmm0
	mulpd	ALPHA, %xmm0
	addpd	-16 * SIZE(Y), %xmm0
	movaps	%xmm0, -16 * SIZE(Y)
	movaps	 -9 * SIZE(X), %xmm0

	SHUFPD_1 %xmm2, %xmm1
	mulpd	ALPHA, %xmm1
	addpd	-14 * SIZE(Y), %xmm1
	movaps	%xmm1, -14 * SIZE(Y)
	movaps	 -7 * SIZE(X), %xmm1

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	SHUFPD_1 %xmm3, %xmm2
	mulpd	ALPHA, %xmm2
	addpd	-12 * SIZE(Y), %xmm2
	movaps	%xmm2, -12 * SIZE(Y)
	movaps	 -5 * SIZE(X), %xmm2

	SHUFPD_1 %xmm0, %xmm3
	mulpd	ALPHA, %xmm3
	addpd	-10 * SIZE(Y), %xmm3
	movaps	%xmm3, -10 * SIZE(Y)
	movaps	 -3 * SIZE(X), %xmm3

#if defined(PREFETCHW) && !defined(FETCH128)
	PREFETCHW (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	SHUFPD_1 %xmm1, %xmm0
	mulpd	ALPHA, %xmm0
	addpd	 -8 * SIZE(Y), %xmm0
	movaps	%xmm0,  -8 * SIZE(Y)
	movaps	 -1 * SIZE(X), %xmm0

	SHUFPD_1 %xmm2, %xmm1
	mulpd	ALPHA, %xmm1
	addpd	 -6 * SIZE(Y), %xmm1
	movaps	%xmm1,  -6 * SIZE(Y)
	movaps	  1 * SIZE(X), %xmm1

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	SHUFPD_1 %xmm3, %xmm2
	mulpd	ALPHA, %xmm2
	addpd	 -4 * SIZE(Y), %xmm2
	movaps	%xmm2,  -4 * SIZE(Y)
	movaps	  3 * SIZE(X), %xmm2

	SHUFPD_1 %xmm0, %xmm3
	mulpd	ALPHA, %xmm3
	addpd	 -2 * SIZE(Y), %xmm3
	movaps	%xmm3,  -2 * SIZE(Y)
	movaps	  5 * SIZE(X), %xmm3

	subl	$-16 * SIZE, X
	subl	$-16 * SIZE, Y
	decl	%eax
	jg	.L21
	ALIGN_3

.L22:
	SHUFPD_1 %xmm1, %xmm0
	mulpd	ALPHA, %xmm0
	addpd	-16 * SIZE(Y), %xmm0
	movaps	%xmm0, -16 * SIZE(Y)
	movaps	 -9 * SIZE(X), %xmm0

	SHUFPD_1 %xmm2, %xmm1
	mulpd	ALPHA, %xmm1
	addpd	-14 * SIZE(Y), %xmm1
	movaps	%xmm1, -14 * SIZE(Y)
	movaps	 -7 * SIZE(X), %xmm1

	SHUFPD_1 %xmm3, %xmm2
	mulpd	ALPHA, %xmm2
	addpd	-12 * SIZE(Y), %xmm2
	movaps	%xmm2, -12 * SIZE(Y)
	movaps	 -5 * SIZE(X), %xmm2

	SHUFPD_1 %xmm0, %xmm3
	mulpd	ALPHA, %xmm3
	addpd	-10 * SIZE(Y), %xmm3
	movaps	%xmm3, -10 * SIZE(Y)
	movaps	 -3 * SIZE(X), %xmm3

	SHUFPD_1 %xmm1, %xmm0
	mulpd	ALPHA, %xmm0
	addpd	 -8 * SIZE(Y), %xmm0
	movaps	%xmm0,  -8 * SIZE(Y)
	movaps	 -1 * SIZE(X), %xmm0

	SHUFPD_1 %xmm2, %xmm1
	mulpd	ALPHA, %xmm1
	addpd	 -6 * SIZE(Y), %xmm1
	movaps	%xmm1,  -6 * SIZE(Y)

	SHUFPD_1 %xmm3, %xmm2
	mulpd	ALPHA, %xmm2
	addpd	 -4 * SIZE(Y), %xmm2
	movaps	%xmm2,  -4 * SIZE(Y)

	SHUFPD_1 %xmm0, %xmm3
	mulpd	ALPHA, %xmm3
	addpd	 -2 * SIZE(Y), %xmm3
	movaps	%xmm3,  -2 * SIZE(Y)

	subl	$-16 * SIZE, X
	subl	$-16 * SIZE, Y
	ALIGN_3

.L23:
	movl	M,  %eax
	andl	$8, %eax
	jle	.L24
	ALIGN_3

	movaps	-15 * SIZE(X), %xmm1
	movaps	-13 * SIZE(X), %xmm2
	movaps	-11 * SIZE(X), %xmm3
	movaps	 -9 * SIZE(X), %xmm4

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

	SHUFPD_1 %xmm4, %xmm3
	mulpd	ALPHA, %xmm3
	addpd	-10 * SIZE(Y), %xmm3
	movaps	%xmm3, -10 * SIZE(Y)

	movaps	%xmm4, %xmm0

	addl	$8 * SIZE, X
	addl	$8 * SIZE, Y
	ALIGN_3

.L24:
	movl	M,  %eax
	andl	$4, %eax
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

	addl	$4 * SIZE, X
	addl	$4 * SIZE, Y
	ALIGN_3

.L25:
	movl	M,  %eax
	andl	$2, %eax
	jle	.L26
	ALIGN_3

	movaps	-15 * SIZE(X), %xmm1
	SHUFPD_1 %xmm1, %xmm0
	mulpd	ALPHA,  %xmm0
	addpd	-16 * SIZE(Y), %xmm0

	movaps	%xmm0, -16 * SIZE(Y)

	addl	$2 * SIZE, X
	addl	$2 * SIZE, Y
	ALIGN_3

.L26:
	movl	M,  %eax
	andl	$1, %eax
	jle	.L29
	ALIGN_3

	movsd	-16 * SIZE(X), %xmm0
	mulsd	ALPHA, %xmm0
	addsd	-16 * SIZE(Y), %xmm0

	movsd	%xmm0, 	-16 * SIZE(Y)
	ALIGN_3

.L29:
	popl	%ebp
	popl	%ebx
	popl	%esi
	popl	%edi
	ret
	ALIGN_3

#else
	movl	M,  %eax
	sarl	$3, %eax
	jle	.L23

	movsd	-16 * SIZE(X), %xmm0
	movhps	-15 * SIZE(X), %xmm0
	movsd	-14 * SIZE(X), %xmm1
	movhps	-13 * SIZE(X), %xmm1
	movsd	-12 * SIZE(X), %xmm2
	movhps	-11 * SIZE(X), %xmm2
	movsd	-10 * SIZE(X), %xmm3
	movhps	 -9 * SIZE(X), %xmm3

	decl	%eax
	jle .L22
	ALIGN_3

.L21:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	mulpd	ALPHA, %xmm0
	addpd	-16 * SIZE(Y), %xmm0
	movaps	%xmm0, -16 * SIZE(Y)

	movsd	 -8 * SIZE(X), %xmm0
	movhps	 -7 * SIZE(X), %xmm0

	mulpd	ALPHA, %xmm1
	addpd	-14 * SIZE(Y), %xmm1
	movaps	%xmm1, -14 * SIZE(Y)

	movsd	 -6 * SIZE(X), %xmm1
	movhps	 -5 * SIZE(X), %xmm1

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	mulpd	ALPHA, %xmm2
	addpd	-12 * SIZE(Y), %xmm2
	movaps	%xmm2, -12 * SIZE(Y)

	movsd	 -4 * SIZE(X), %xmm2
	movhps	 -3 * SIZE(X), %xmm2

	mulpd	ALPHA, %xmm3
	addpd	-10 * SIZE(Y), %xmm3
	movaps	%xmm3, -10 * SIZE(Y)

	movsd	 -2 * SIZE(X), %xmm3
	movhps	 -1 * SIZE(X), %xmm3

	subl	$-8 * SIZE, Y
	subl	$-8 * SIZE, X
	decl	%eax
	jg	.L21
	ALIGN_3

.L22:
	mulpd	ALPHA, %xmm0
	addpd	-16 * SIZE(Y), %xmm0
	movaps	%xmm0, -16 * SIZE(Y)

	mulpd	ALPHA, %xmm1
	addpd	-14 * SIZE(Y), %xmm1
	movaps	%xmm1, -14 * SIZE(Y)

	mulpd	ALPHA, %xmm2
	addpd	-12 * SIZE(Y), %xmm2
	movaps	%xmm2, -12 * SIZE(Y)

	mulpd	ALPHA, %xmm3
	addpd	-10 * SIZE(Y), %xmm3
	movaps	%xmm3, -10 * SIZE(Y)

	subl	$-8 * SIZE, Y
	subl	$-8 * SIZE, X
	ALIGN_3

.L23:
	movl	M,  %eax
	andl	$4, %eax
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

	addl	$4 * SIZE, X
	addl	$4 * SIZE, Y
	ALIGN_3

.L25:
	movl	M,  %eax
	andl	$2, %eax
	jle	.L26
	ALIGN_3

	movsd	-16 * SIZE(X), %xmm0
	movhps	-15 * SIZE(X), %xmm0
	mulpd	ALPHA, %xmm0
	addpd	-16 * SIZE(Y), %xmm0
	movaps	%xmm0, -16 * SIZE(Y)

	addl	$2 * SIZE, X
	addl	$2 * SIZE, Y
	ALIGN_3

.L26:
	movl	M,  %eax
	andl	$1, %eax
	jle	.L29
	ALIGN_3

	movsd	-16 * SIZE(X), %xmm0
	mulsd	ALPHA, %xmm0
	addsd	-16 * SIZE(Y), %xmm0

	movsd	%xmm0, 	-16 * SIZE(Y)
	ALIGN_3

.L29:
	popl	%ebp
	popl	%ebx
	popl	%esi
	popl	%edi
	ret
	ALIGN_3
#endif

.L40:
	movl	Y, YY
	movl	M,  %eax
//If incx==0 || incy==0, avoid unloop.
	cmpl	$0, INCX
	je  .L46
	cmpl	$0, INCY
	je  .L46

	sarl	$3, %eax
	jle	.L45
	ALIGN_3

.L41:
	movsd	0 * SIZE(X), %xmm0
	addl	INCX, X
	movhpd	0 * SIZE(X), %xmm0
	addl	INCX, X
	mulpd	ALPHA, %xmm0

	movsd	0 * SIZE(YY), %xmm6
	addl	INCY, YY
	movhpd	0 * SIZE(YY), %xmm6
	addl	INCY, YY
	addpd	%xmm6, %xmm0

	movsd	0 * SIZE(X), %xmm1
	addl	INCX, X
	movhpd	0 * SIZE(X), %xmm1
	addl	INCX, X
	mulpd	ALPHA, %xmm1

	movsd	0 * SIZE(YY), %xmm6
	addl	INCY, YY
	movhpd	0 * SIZE(YY), %xmm6
	addl	INCY, YY
	addpd	%xmm6, %xmm1

	movsd	0 * SIZE(X), %xmm2
	addl	INCX, X
	movhpd	0 * SIZE(X), %xmm2
	addl	INCX, X
	mulpd	ALPHA, %xmm2

	movsd	0 * SIZE(YY), %xmm6
	addl	INCY, YY
	movhpd	0 * SIZE(YY), %xmm6
	addl	INCY, YY
	addpd	%xmm6, %xmm2

	movsd	0 * SIZE(X), %xmm3
	addl	INCX, X
	movhpd	0 * SIZE(X), %xmm3
	addl	INCX, X
	mulpd	ALPHA, %xmm3

	movsd	0 * SIZE(YY), %xmm6
	addl	INCY, YY
	movhpd	0 * SIZE(YY), %xmm6
	addl	INCY, YY
	addpd	%xmm6, %xmm3

	movsd	%xmm0, 0 * SIZE(Y)
	addl	INCY, Y
	movhpd	%xmm0, 0 * SIZE(Y)
	addl	INCY, Y
	movsd	%xmm1, 0 * SIZE(Y)
	addl	INCY, Y
	movhpd	%xmm1, 0 * SIZE(Y)
	addl	INCY, Y
	movsd	%xmm2, 0 * SIZE(Y)
	addl	INCY, Y
	movhpd	%xmm2, 0 * SIZE(Y)
	addl	INCY, Y
	movsd	%xmm3, 0 * SIZE(Y)
	addl	INCY, Y
	movhpd	%xmm3, 0 * SIZE(Y)
	addl	INCY, Y

	decl	%eax
	jg	.L41
	ALIGN_3

.L45:
	movl	M,  %eax
	andl	$7, %eax
	jle	.L47
	ALIGN_3

.L46:
	movsd	(X), %xmm0
	addl	INCX, X
	mulsd	ALPHA, %xmm0
	addsd	(Y), %xmm0
	movsd	%xmm0, (Y)
	addl	INCY, Y
	decl	%eax
	jg	.L46
	ALIGN_3

.L47:
	popl	%ebp
	popl	%ebx
	popl	%esi
	popl	%edi
	ret

	EPILOGUE
