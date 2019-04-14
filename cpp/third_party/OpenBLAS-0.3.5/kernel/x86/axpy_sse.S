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
#define STACK_ALPHA 	16 + STACK + ARGS(%esp)
#define STACK_X		20 + STACK + ARGS(%esp)
#define STACK_INCX	24 + STACK + ARGS(%esp)
#define STACK_Y		28 + STACK + ARGS(%esp)
#define STACK_INCY	32 + STACK + ARGS(%esp)

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
	movss	STACK_ALPHA, ALPHA
	movl	STACK_X,    X
	movl	STACK_INCX, INCX
	movl	STACK_Y,    Y
	movl	STACK_INCY, INCY

	shufps	$0, ALPHA, ALPHA

	leal	(, INCX, SIZE), INCX
	leal	(, INCY, SIZE), INCY

	testl	M, M
	jle	.L19

	cmpl	$SIZE, INCX
	jne	.L50
	cmpl	$SIZE, INCY
	jne	.L50

	subl	$-32 * SIZE, X
	subl	$-32 * SIZE, Y

	cmpl	$3, M
	jle	.L16

	testl	$SIZE, Y
	je	.L00

	movss	-32 * SIZE(X), %xmm0
	mulss	ALPHA, %xmm0
	addss	-32 * SIZE(Y), %xmm0
	movss	%xmm0, -32 * SIZE(Y)
	addl	$1 * SIZE, X
	addl	$1 * SIZE, Y
	decl	M
	jle	.L19
	ALIGN_3

.L00:
	testl	$SIZE * 2, Y
	je	.L10

	movsd	-32 * SIZE(X), %xmm0
	movsd	-32 * SIZE(Y), %xmm4
	mulps	ALPHA, %xmm0
	addps	%xmm4, %xmm0
	movsd	%xmm0, -32 * SIZE(Y)

	addl	$2 * SIZE, X
	addl	$2 * SIZE, Y
	subl	$2, M
	jle	.L19
	ALIGN_3

.L10:
	testl	$SIZE * 3, X
	jne	.L20

	movl	M,  %eax
	sarl	$5, %eax
	jle	.L13

	movaps	-32 * SIZE(X), %xmm0
	movaps	-28 * SIZE(X), %xmm1
	movaps	-24 * SIZE(X), %xmm2
	movaps	-20 * SIZE(X), %xmm3

	decl	%eax
	jle .L12
	ALIGN_4

.L11:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	mulps	ALPHA, %xmm0
	addps	-32 * SIZE(Y), %xmm0
	movaps	%xmm0, -32 * SIZE(Y)
	movaps	-16 * SIZE(X), %xmm0

	mulps	ALPHA, %xmm1
	addps	-28 * SIZE(Y), %xmm1
	movaps	%xmm1, -28 * SIZE(Y)
	movaps	-12 * SIZE(X), %xmm1

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	mulps	ALPHA, %xmm2
	addps	-24 * SIZE(Y), %xmm2
	movaps	%xmm2, -24 * SIZE(Y)
	movaps	 -8 * SIZE(X), %xmm2

	mulps	ALPHA, %xmm3
	addps	-20 * SIZE(Y), %xmm3
	movaps	%xmm3, -20 * SIZE(Y)
	movaps	 -4 * SIZE(X), %xmm3

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	mulps	ALPHA, %xmm0
	addps	-16 * SIZE(Y), %xmm0
	movaps	%xmm0, -16 * SIZE(Y)
	movaps	  0 * SIZE(X), %xmm0

	mulps	ALPHA, %xmm1
	addps	-12 * SIZE(Y), %xmm1
	movaps	%xmm1, -12 * SIZE(Y)
	movaps	  4 * SIZE(X), %xmm1

#if defined(PREFETCHW) && !defined(FETCH128)
	PREFETCHW (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	mulps	ALPHA, %xmm2
	addps	 -8 * SIZE(Y), %xmm2
	movaps	%xmm2,  -8 * SIZE(Y)
	movaps	  8 * SIZE(X), %xmm2

	mulps	ALPHA, %xmm3
	addps	 -4 * SIZE(Y), %xmm3
	movaps	%xmm3,  -4 * SIZE(Y)
	movaps	 12 * SIZE(X), %xmm3

	subl	$-32 * SIZE, X
	subl	$-32 * SIZE, Y
	decl	%eax
	jg	.L11
	ALIGN_3

.L12:
	mulps	ALPHA, %xmm0
	addps	-32 * SIZE(Y), %xmm0
	movaps	%xmm0, -32 * SIZE(Y)
	movaps	-16 * SIZE(X), %xmm0

	mulps	ALPHA, %xmm1
	addps	-28 * SIZE(Y), %xmm1
	movaps	%xmm1, -28 * SIZE(Y)
	movaps	-12 * SIZE(X), %xmm1

	mulps	ALPHA, %xmm2
	addps	-24 * SIZE(Y), %xmm2
	movaps	%xmm2, -24 * SIZE(Y)
	movaps	 -8 * SIZE(X), %xmm2

	mulps	ALPHA, %xmm3
	addps	-20 * SIZE(Y), %xmm3
	movaps	%xmm3, -20 * SIZE(Y)
	movaps	 -4 * SIZE(X), %xmm3

	mulps	ALPHA, %xmm0
	addps	-16 * SIZE(Y), %xmm0
	movaps	%xmm0, -16 * SIZE(Y)

	mulps	ALPHA, %xmm1
	addps	-12 * SIZE(Y), %xmm1
	movaps	%xmm1, -12 * SIZE(Y)

	mulps	ALPHA, %xmm2
	addps	 -8 * SIZE(Y), %xmm2
	movaps	%xmm2,  -8 * SIZE(Y)

	mulps	ALPHA, %xmm3
	addps	 -4 * SIZE(Y), %xmm3
	movaps	%xmm3,  -4 * SIZE(Y)

	subl	$-32 * SIZE, X
	subl	$-32 * SIZE, Y
	ALIGN_3

.L13:
	movl	M,  %eax
	andl	$16, %eax
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

	addl	$16 * SIZE, X
	addl	$16 * SIZE, Y
	ALIGN_3

.L14:
	movl	M,  %eax
	andl	$8, %eax
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

	addl	$8 * SIZE, X
	addl	$8 * SIZE, Y
	ALIGN_3

.L15:
	movl	M,  %eax
	andl	$4, %eax
	jle	.L16
	ALIGN_3

	movaps	-32 * SIZE(X), %xmm0

	mulps	ALPHA, %xmm0

	addps	-32 * SIZE(Y), %xmm0

	movaps	%xmm0, -32 * SIZE(Y)

	addl	$4 * SIZE, X
	addl	$4 * SIZE, Y
	ALIGN_3

.L16:
	movl	M,  %eax
	andl	$2, %eax
	jle	.L17
	ALIGN_3

	movsd	-32 * SIZE(X), %xmm0
	movsd	-32 * SIZE(Y), %xmm4

	mulps	ALPHA, %xmm0
	addps	%xmm4, %xmm0

	movsd	%xmm0, -32 * SIZE(Y)

	addl	$2 * SIZE, X
	addl	$2 * SIZE, Y
	ALIGN_3

.L17:
	movl	M,  %eax
	andl	$1, %eax
	jle	.L19
	ALIGN_3

	movss	-32 * SIZE(X), %xmm0
	mulss	ALPHA, %xmm0
	addss	-32 * SIZE(Y), %xmm0

	movss	%xmm0, 	-32 * SIZE(Y)
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

	testl	$SIZE, X
	jne	.L30

	movhps	-32 * SIZE(X), %xmm0

	movl	M,  %eax
	sarl	$5, %eax
	jle	.L23

	movaps	-30 * SIZE(X), %xmm1
	movaps	-26 * SIZE(X), %xmm2
	movaps	-22 * SIZE(X), %xmm3

	decl	%eax
	jle .L22
	ALIGN_4

.L21:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	SHUFPD_1 %xmm1, %xmm0
	mulps	ALPHA, %xmm0
	addps	-32 * SIZE(Y), %xmm0
	movaps	%xmm0, -32 * SIZE(Y)
	movaps	-18 * SIZE(X), %xmm0

	SHUFPD_1 %xmm2, %xmm1
	mulps	ALPHA, %xmm1
	addps	-28 * SIZE(Y), %xmm1
	movaps	%xmm1, -28 * SIZE(Y)
	movaps	-14 * SIZE(X), %xmm1

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	SHUFPD_1 %xmm3, %xmm2
	mulps	ALPHA, %xmm2
	addps	-24 * SIZE(Y), %xmm2
	movaps	%xmm2, -24 * SIZE(Y)
	movaps	-10 * SIZE(X), %xmm2

	SHUFPD_1 %xmm0, %xmm3
	mulps	ALPHA, %xmm3
	addps	-20 * SIZE(Y), %xmm3
	movaps	%xmm3, -20 * SIZE(Y)
	movaps	 -6 * SIZE(X), %xmm3

#if defined(PREFETCHW) && !defined(FETCH128)
	PREFETCHW (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	SHUFPD_1 %xmm1, %xmm0
	mulps	ALPHA, %xmm0
	addps	-16 * SIZE(Y), %xmm0
	movaps	%xmm0, -16 * SIZE(Y)
	movaps	 -2 * SIZE(X), %xmm0

	SHUFPD_1 %xmm2, %xmm1
	mulps	ALPHA, %xmm1
	addps	-12 * SIZE(Y), %xmm1
	movaps	%xmm1, -12 * SIZE(Y)
	movaps	  2 * SIZE(X), %xmm1

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	SHUFPD_1 %xmm3, %xmm2
	mulps	ALPHA, %xmm2
	addps	 -8 * SIZE(Y), %xmm2
	movaps	%xmm2,  -8 * SIZE(Y)
	movaps	  6 * SIZE(X), %xmm2

	SHUFPD_1 %xmm0, %xmm3
	mulps	ALPHA, %xmm3
	addps	 -4 * SIZE(Y), %xmm3
	movaps	%xmm3,  -4 * SIZE(Y)
	movaps	 10 * SIZE(X), %xmm3

	subl	$-32 * SIZE, X
	subl	$-32 * SIZE, Y
	decl	%eax
	jg	.L21
	ALIGN_3

.L22:
	SHUFPD_1 %xmm1, %xmm0
	mulps	ALPHA, %xmm0
	addps	-32 * SIZE(Y), %xmm0
	movaps	%xmm0, -32 * SIZE(Y)
	movaps	-18 * SIZE(X), %xmm0

	SHUFPD_1 %xmm2, %xmm1
	mulps	ALPHA, %xmm1
	addps	-28 * SIZE(Y), %xmm1
	movaps	%xmm1, -28 * SIZE(Y)
	movaps	-14 * SIZE(X), %xmm1

	SHUFPD_1 %xmm3, %xmm2
	mulps	ALPHA, %xmm2
	addps	-24 * SIZE(Y), %xmm2
	movaps	%xmm2, -24 * SIZE(Y)
	movaps	-10 * SIZE(X), %xmm2

	SHUFPD_1 %xmm0, %xmm3
	mulps	ALPHA, %xmm3
	addps	-20 * SIZE(Y), %xmm3
	movaps	%xmm3, -20 * SIZE(Y)
	movaps	 -6 * SIZE(X), %xmm3

	SHUFPD_1 %xmm1, %xmm0
	mulps	ALPHA, %xmm0
	addps	-16 * SIZE(Y), %xmm0
	movaps	%xmm0, -16 * SIZE(Y)
	movaps	 -2 * SIZE(X), %xmm0

	SHUFPD_1 %xmm2, %xmm1
	mulps	ALPHA, %xmm1
	addps	-12 * SIZE(Y), %xmm1
	movaps	%xmm1, -12 * SIZE(Y)

	SHUFPD_1 %xmm3, %xmm2
	mulps	ALPHA, %xmm2
	addps	 -8 * SIZE(Y), %xmm2
	movaps	%xmm2,  -8 * SIZE(Y)

	SHUFPD_1 %xmm0, %xmm3
	mulps	ALPHA, %xmm3
	addps	 -4 * SIZE(Y), %xmm3
	movaps	%xmm3,  -4 * SIZE(Y)

	subl	$-32 * SIZE, X
	subl	$-32 * SIZE, Y
	ALIGN_3

.L23:
	movl	M,  %eax
	andl	$16, %eax
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

	addl	$16 * SIZE, X
	addl	$16 * SIZE, Y
	ALIGN_3

.L24:
	movl	M,  %eax
	andl	$8, %eax
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

	addl	$8 * SIZE, X
	addl	$8 * SIZE, Y
	ALIGN_3

.L25:
	movl	M,  %eax
	andl	$4, %eax
	jle	.L26
	ALIGN_3

	movaps	-30 * SIZE(X), %xmm1

	SHUFPD_1 %xmm1, %xmm0
	mulps	ALPHA, %xmm0

	addps	-32 * SIZE(Y), %xmm0

	movaps	%xmm0, -32 * SIZE(Y)

	addl	$4 * SIZE, X
	addl	$4 * SIZE, Y
	ALIGN_3

.L26:
	movl	M,  %eax
	andl	$2, %eax
	jle	.L27
	ALIGN_3

	movsd	-32 * SIZE(X), %xmm0
	movsd	-32 * SIZE(Y), %xmm4

	mulps	ALPHA, %xmm0
	addps	%xmm4, %xmm0

	movsd	%xmm0, -32 * SIZE(Y)

	addl	$2 * SIZE, X
	addl	$2 * SIZE, Y
	ALIGN_3

.L27:
	movl	M,  %eax
	andl	$1, %eax
	jle	.L29
	ALIGN_3

	movss	-32 * SIZE(X), %xmm0
	mulss	ALPHA, %xmm0
	addss	-32 * SIZE(Y), %xmm0

	movss	%xmm0, 	-32 * SIZE(Y)
	addl	$SIZE, Y
	ALIGN_3

.L29:
	popl	%ebp
	popl	%ebx
	popl	%esi
	popl	%edi
	ret
	ALIGN_3

.L30:
	testl	$2 * SIZE, X
	jne	.L40

	movaps	-33 * SIZE(X), %xmm0

	movl	M,  %eax
	sarl	$5, %eax
	jle	.L33

	movaps	-29 * SIZE(X), %xmm1
	movaps	-25 * SIZE(X), %xmm2
	movaps	-21 * SIZE(X), %xmm3

	decl	%eax
	jle .L32
	ALIGN_4

.L31:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	movss	%xmm1, %xmm0
	SHUFPS_39 %xmm0, %xmm0
	mulps	ALPHA, %xmm0
	addps	-32 * SIZE(Y), %xmm0
	movaps	%xmm0, -32 * SIZE(Y)
	movaps	-17 * SIZE(X), %xmm0

	movss	%xmm2, %xmm1
	SHUFPS_39 %xmm1, %xmm1
	mulps	ALPHA, %xmm1
	addps	-28 * SIZE(Y), %xmm1
	movaps	%xmm1, -28 * SIZE(Y)
	movaps	-13 * SIZE(X), %xmm1

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movss	%xmm3, %xmm2
	SHUFPS_39 %xmm2, %xmm2
	mulps	ALPHA, %xmm2
	addps	-24 * SIZE(Y), %xmm2
	movaps	%xmm2, -24 * SIZE(Y)
	movaps	 -9 * SIZE(X), %xmm2

	movss	%xmm0, %xmm3
	SHUFPS_39 %xmm3, %xmm3
	mulps	ALPHA, %xmm3
	addps	-20 * SIZE(Y), %xmm3
	movaps	%xmm3, -20 * SIZE(Y)
	movaps	 -5 * SIZE(X), %xmm3

#if defined(PREFETCHW) && !defined(FETCH128)
	PREFETCHW (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	movss	%xmm1, %xmm0
	SHUFPS_39 %xmm0, %xmm0
	mulps	ALPHA, %xmm0
	addps	-16 * SIZE(Y), %xmm0
	movaps	%xmm0, -16 * SIZE(Y)
	movaps	 -1 * SIZE(X), %xmm0

	movss	%xmm2, %xmm1
	SHUFPS_39 %xmm1, %xmm1
	mulps	ALPHA, %xmm1
	addps	-12 * SIZE(Y), %xmm1
	movaps	%xmm1, -12 * SIZE(Y)
 	movaps	  3 * SIZE(X), %xmm1

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	movss	%xmm3, %xmm2
	SHUFPS_39 %xmm2, %xmm2
	mulps	ALPHA, %xmm2
	addps	-8 * SIZE(Y), %xmm2
	movaps	%xmm2,  -8 * SIZE(Y)
	movaps	  7 * SIZE(X), %xmm2

	movss	%xmm0, %xmm3
	SHUFPS_39 %xmm3, %xmm3
	mulps	ALPHA, %xmm3
	addps	-4 * SIZE(Y), %xmm3
	movaps	%xmm3,  -4 * SIZE(Y)
	movaps	 11 * SIZE(X), %xmm3

	subl	$-32 * SIZE, X
	subl	$-32 * SIZE, Y
	decl	%eax
	jg	.L31
	ALIGN_3

.L32:
	movss	%xmm1, %xmm0
	SHUFPS_39 %xmm0, %xmm0
	mulps	ALPHA, %xmm0
	addps	-32 * SIZE(Y), %xmm0
	movaps	%xmm0, -32 * SIZE(Y)
	movaps	-17 * SIZE(X), %xmm0

	movss	%xmm2, %xmm1
	SHUFPS_39 %xmm1, %xmm1
	mulps	ALPHA, %xmm1
	addps	-28 * SIZE(Y), %xmm1
	movaps	%xmm1, -28 * SIZE(Y)
	movaps	-13 * SIZE(X), %xmm1

	movss	%xmm3, %xmm2
	SHUFPS_39 %xmm2, %xmm2
	mulps	ALPHA, %xmm2
	addps	-24 * SIZE(Y), %xmm2
	movaps	%xmm2, -24 * SIZE(Y)
	movaps	 -9 * SIZE(X), %xmm2

	movss	%xmm0, %xmm3
	SHUFPS_39 %xmm3, %xmm3
	mulps	ALPHA, %xmm3
	addps	-20 * SIZE(Y), %xmm3
	movaps	%xmm3, -20 * SIZE(Y)
	movaps	 -5 * SIZE(X), %xmm3

	movss	%xmm1, %xmm0
	SHUFPS_39 %xmm0, %xmm0
	mulps	ALPHA, %xmm0
	addps	-16 * SIZE(Y), %xmm0
	movaps	%xmm0, -16 * SIZE(Y)
	movaps	 -1 * SIZE(X), %xmm0

	movss	%xmm2, %xmm1
	SHUFPS_39 %xmm1, %xmm1
	mulps	ALPHA, %xmm1
	addps	-12 * SIZE(Y), %xmm1
	movaps	%xmm1, -12 * SIZE(Y)

	movss	%xmm3, %xmm2
	SHUFPS_39 %xmm2, %xmm2
	mulps	ALPHA, %xmm2
	addps	-8 * SIZE(Y), %xmm2
	movaps	%xmm2,  -8 * SIZE(Y)

	movss	%xmm0, %xmm3
	SHUFPS_39 %xmm3, %xmm3
	mulps	ALPHA, %xmm3
	addps	-4 * SIZE(Y), %xmm3
	movaps	%xmm3,  -4 * SIZE(Y)

	subl	$-32 * SIZE, X
	subl	$-32 * SIZE, Y
	ALIGN_3

.L33:
	movl	M,  %eax
	andl	$16, %eax
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

	addl	$16 * SIZE, X
	addl	$16 * SIZE, Y
	ALIGN_3

.L34:
	movl	M,  %eax
	andl	$8, %eax
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

	addl	$8 * SIZE, X
	addl	$8 * SIZE, Y
	ALIGN_3

.L35:
	movl	M,  %eax
	andl	$4, %eax
	jle	.L36
	ALIGN_3

	movaps	-29 * SIZE(X), %xmm1

	movss	%xmm1, %xmm0
	SHUFPS_39 %xmm0, %xmm0
	mulps	ALPHA, %xmm0

	addps	-32 * SIZE(Y), %xmm0

	movaps	%xmm0, -32 * SIZE(Y)

	addl	$4 * SIZE, X
	addl	$4 * SIZE, Y
	ALIGN_3

.L36:
	movl	M,  %eax
	andl	$2, %eax
	jle	.L37
	ALIGN_3

	movsd	-32 * SIZE(X), %xmm0
	movsd	-32 * SIZE(Y), %xmm4

	mulps	ALPHA, %xmm0
	addps	%xmm4, %xmm0

	movsd	%xmm0, -32 * SIZE(Y)

	addl	$2 * SIZE, X
	addl	$2 * SIZE, Y
	ALIGN_3

.L37:
	movl	M,  %eax
	andl	$1, %eax
	jle	.L39
	ALIGN_3

	movss	-32 * SIZE(X), %xmm0
	mulss	ALPHA, %xmm0
	addss	-32 * SIZE(Y), %xmm0

	movss	%xmm0, 	-32 * SIZE(Y)
	addl	$SIZE, Y
	ALIGN_3

.L39:
	popl	%ebp
	popl	%ebx
	popl	%esi
	popl	%edi
	ret
	ALIGN_3

.L40:
	movaps	-35 * SIZE(X), %xmm0

	movl	M,  %eax
	sarl	$5, %eax
	jle	.L43

	movaps	-31 * SIZE(X), %xmm1
	movaps	-27 * SIZE(X), %xmm2
	movaps	-23 * SIZE(X), %xmm3

	decl	%eax
	jle .L42
	ALIGN_4

.L41:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	movss	%xmm1, %xmm0
	shufps	$0x93, %xmm1, %xmm0
	mulps	ALPHA, %xmm0
	addps	-32 * SIZE(Y), %xmm0
	movaps	%xmm0, -32 * SIZE(Y)
	movaps	-19 * SIZE(X), %xmm0

	movss	%xmm2, %xmm1
	shufps	$0x93, %xmm2, %xmm1
	mulps	ALPHA, %xmm1
	addps	-28 * SIZE(Y), %xmm1
	movaps	%xmm1, -28 * SIZE(Y)
	movaps	-15 * SIZE(X), %xmm1

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movss	%xmm3, %xmm2
	shufps	$0x93, %xmm3, %xmm2
	mulps	ALPHA, %xmm2
	addps	-24 * SIZE(Y), %xmm2
	movaps	%xmm2, -24 * SIZE(Y)
	movaps	-11 * SIZE(X), %xmm2

	movss	%xmm0, %xmm3
	shufps	$0x93, %xmm0, %xmm3
	mulps	ALPHA, %xmm3
	addps	-20 * SIZE(Y), %xmm3
	movaps	%xmm3, -20 * SIZE(Y)
	movaps	 -7 * SIZE(X), %xmm3

#if defined(PREFETCHW) && !defined(FETCH128)
	PREFETCHW (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	movss	%xmm1, %xmm0
	shufps	$0x93, %xmm1, %xmm0
	mulps	ALPHA, %xmm0
	addps	-16 * SIZE(Y), %xmm0
	movaps	%xmm0, -16 * SIZE(Y)
	movaps	 -3 * SIZE(X), %xmm0

	movss	%xmm2, %xmm1
	shufps	$0x93, %xmm2, %xmm1
	mulps	ALPHA, %xmm1
	addps	-12 * SIZE(Y), %xmm1
	movaps	%xmm1, -12 * SIZE(Y)
	movaps	  1 * SIZE(X), %xmm1

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	movss	%xmm3, %xmm2
	shufps	$0x93, %xmm3, %xmm2
	mulps	ALPHA, %xmm2
	addps	 -8 * SIZE(Y), %xmm2
	movaps	%xmm2, -8 * SIZE(Y)
	movaps	  5 * SIZE(X), %xmm2

	movss	%xmm0, %xmm3
	shufps	$0x93, %xmm0, %xmm3
	mulps	ALPHA, %xmm3
	addps	 -4 * SIZE(Y), %xmm3
	movaps	%xmm3, -4 * SIZE(Y)
	movaps	  9 * SIZE(X), %xmm3

	subl	$-32 * SIZE, X
	subl	$-32 * SIZE, Y
	decl	%eax
	jg	.L41
	ALIGN_3

.L42:
	movss	%xmm1, %xmm0
	shufps	$0x93, %xmm1, %xmm0
	mulps	ALPHA, %xmm0
	addps	-32 * SIZE(Y), %xmm0
	movaps	%xmm0, -32 * SIZE(Y)
	movaps	-19 * SIZE(X), %xmm0

	movss	%xmm2, %xmm1
	shufps	$0x93, %xmm2, %xmm1
	mulps	ALPHA, %xmm1
	addps	-28 * SIZE(Y), %xmm1
	movaps	%xmm1, -28 * SIZE(Y)
	movaps	-15 * SIZE(X), %xmm1

	movss	%xmm3, %xmm2
	shufps	$0x93, %xmm3, %xmm2
	mulps	ALPHA, %xmm2
	addps	-24 * SIZE(Y), %xmm2
	movaps	%xmm2, -24 * SIZE(Y)
	movaps	-11 * SIZE(X), %xmm2

	movss	%xmm0, %xmm3
	shufps	$0x93, %xmm0, %xmm3
	mulps	ALPHA, %xmm3
	addps	-20 * SIZE(Y), %xmm3
	movaps	%xmm3, -20 * SIZE(Y)
	movaps	 -7 * SIZE(X), %xmm3

	movss	%xmm1, %xmm0
	shufps	$0x93, %xmm1, %xmm0
	mulps	ALPHA, %xmm0
	addps	-16 * SIZE(Y), %xmm0
	movaps	%xmm0, -16 * SIZE(Y)
	movaps	 -3 * SIZE(X), %xmm0

	movss	%xmm2, %xmm1
	shufps	$0x93, %xmm2, %xmm1
	mulps	ALPHA, %xmm1
	addps	-12 * SIZE(Y), %xmm1
	movaps	%xmm1, -12 * SIZE(Y)

	movss	%xmm3, %xmm2
	shufps	$0x93, %xmm3, %xmm2
	mulps	ALPHA, %xmm2
	addps	 -8 * SIZE(Y), %xmm2
	movaps	%xmm2, -8 * SIZE(Y)

	movss	%xmm0, %xmm3
	shufps	$0x93, %xmm0, %xmm3
	mulps	ALPHA, %xmm3
	addps	 -4 * SIZE(Y), %xmm3
	movaps	%xmm3, -4 * SIZE(Y)

	subl	$-32 * SIZE, X
	subl	$-32 * SIZE, Y
	ALIGN_3

.L43:
	movl	M,  %eax
	andl	$16, %eax
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

	addl	$16 * SIZE, X
	addl	$16 * SIZE, Y
	ALIGN_3

.L44:
	movl	M,  %eax
	andl	$8, %eax
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

	addl	$8 * SIZE, X
	addl	$8 * SIZE, Y
	ALIGN_3

.L45:
	movl	M,  %eax
	andl	$4, %eax
	jle	.L46
	ALIGN_3

	movaps	-31 * SIZE(X), %xmm1

	movss	%xmm1, %xmm0
	shufps	$0x93, %xmm1, %xmm0
	mulps	ALPHA, %xmm0

	addps	-32 * SIZE(Y), %xmm0

	movaps	%xmm0, -32 * SIZE(Y)

	addl	$4 * SIZE, X
	addl	$4 * SIZE, Y
	ALIGN_3

.L46:
	movl	M,  %eax
	andl	$2, %eax
	jle	.L47
	ALIGN_3

	movsd	-32 * SIZE(X), %xmm0
	movsd	-32 * SIZE(Y), %xmm4

	mulps	ALPHA, %xmm0
	addps	%xmm4, %xmm0

	movsd	%xmm0, -32 * SIZE(Y)

	addl	$2 * SIZE, X
	addl	$2 * SIZE, Y
	ALIGN_3

.L47:
	movl	M,  %eax
	andl	$1, %eax
	jle	.L49
	ALIGN_3

	movss	-32 * SIZE(X), %xmm0
	mulss	ALPHA, %xmm0
	addss	-32 * SIZE(Y), %xmm0

	movss	%xmm0, 	-32 * SIZE(Y)
	addl	$SIZE, Y
	ALIGN_3

.L49:
	popl	%ebp
	popl	%ebx
	popl	%esi
	popl	%edi
	ret
#else

	movl	M,  %eax
	sarl	$5, %eax
	jle	.L23

	movsd	-32 * SIZE(X), %xmm0
	movhps	-30 * SIZE(X), %xmm0
	movsd	-28 * SIZE(X), %xmm1
	movhps	-26 * SIZE(X), %xmm1
	movsd	-24 * SIZE(X), %xmm2
	movhps	-22 * SIZE(X), %xmm2
	movsd	-20 * SIZE(X), %xmm3
	movhps	-18 * SIZE(X), %xmm3

	decl	%eax
	jle .L22
	ALIGN_4

.L21:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	mulps	ALPHA, %xmm0
	addps	-32 * SIZE(Y), %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	movsd	-16 * SIZE(X), %xmm0
	movhps	-14 * SIZE(X), %xmm0

	mulps	ALPHA, %xmm1
	addps	-28 * SIZE(Y), %xmm1
	movaps	%xmm1, -28 * SIZE(Y)

	movsd	-12 * SIZE(X), %xmm1
	movhps	-10 * SIZE(X), %xmm1

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	mulps	ALPHA, %xmm2
	addps	-24 * SIZE(Y), %xmm2
	movaps	%xmm2, -24 * SIZE(Y)

	movsd	 -8 * SIZE(X), %xmm2
	movhps	 -6 * SIZE(X), %xmm2

	mulps	ALPHA, %xmm3
	addps	-20 * SIZE(Y), %xmm3
	movaps	%xmm3, -20 * SIZE(Y)

	movsd	 -4 * SIZE(X), %xmm3
	movhps	 -2 * SIZE(X), %xmm3

#if defined(PREFETCHW) && !defined(FETCH128)
	PREFETCHW (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	mulps	ALPHA, %xmm0
	addps	-16 * SIZE(Y), %xmm0
	movaps	%xmm0, -16 * SIZE(Y)

	movsd	  0 * SIZE(X), %xmm0
	movhps	  2 * SIZE(X), %xmm0

	mulps	ALPHA, %xmm1
	addps	-12 * SIZE(Y), %xmm1
	movaps	%xmm1, -12 * SIZE(Y)

	movsd	  4 * SIZE(X), %xmm1
	movhps	  6 * SIZE(X), %xmm1

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	mulps	ALPHA, %xmm2
	addps	 -8 * SIZE(Y), %xmm2
	movaps	%xmm2,  -8 * SIZE(Y)

	movsd	  8 * SIZE(X), %xmm2
	movhps	 10 * SIZE(X), %xmm2

	mulps	ALPHA, %xmm3
	addps	 -4 * SIZE(Y), %xmm3
	movaps	%xmm3,  -4 * SIZE(Y)

	movsd	 12 * SIZE(X), %xmm3
	movhps	 14 * SIZE(X), %xmm3

	subl	$-32 * SIZE, X
	subl	$-32 * SIZE, Y
	decl	%eax
	jg	.L21
	ALIGN_3

.L22:
	mulps	ALPHA, %xmm0
	addps	-32 * SIZE(Y), %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	movsd	-16 * SIZE(X), %xmm0
	movhps	-14 * SIZE(X), %xmm0

	mulps	ALPHA, %xmm1
	addps	-28 * SIZE(Y), %xmm1
	movaps	%xmm1, -28 * SIZE(Y)

	movsd	-12 * SIZE(X), %xmm1
	movhps	-10 * SIZE(X), %xmm1

	mulps	ALPHA, %xmm2
	addps	-24 * SIZE(Y), %xmm2
	movaps	%xmm2, -24 * SIZE(Y)

	movsd	 -8 * SIZE(X), %xmm2
	movhps	 -6 * SIZE(X), %xmm2

	mulps	ALPHA, %xmm3
	addps	-20 * SIZE(Y), %xmm3
	movaps	%xmm3, -20 * SIZE(Y)

	movsd	 -4 * SIZE(X), %xmm3
	movhps	 -2 * SIZE(X), %xmm3

	mulps	ALPHA, %xmm0
	addps	-16 * SIZE(Y), %xmm0
	movaps	%xmm0, -16 * SIZE(Y)

	mulps	ALPHA, %xmm1
	addps	-12 * SIZE(Y), %xmm1
	movaps	%xmm1, -12 * SIZE(Y)

	mulps	ALPHA, %xmm2
	addps	 -8 * SIZE(Y), %xmm2
	movaps	%xmm2,  -8 * SIZE(Y)

	mulps	ALPHA, %xmm3
	addps	 -4 * SIZE(Y), %xmm3
	movaps	%xmm3,  -4 * SIZE(Y)

	subl	$-32 * SIZE, X
	subl	$-32 * SIZE, Y
	ALIGN_3

.L23:
	movl	M,  %eax
	andl	$16, %eax
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

	addl	$16 * SIZE, X
	addl	$16 * SIZE, Y
	ALIGN_3

.L24:
	movl	M,  %eax
	andl	$8, %eax
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

	addl	$8 * SIZE, X
	addl	$8 * SIZE, Y
	ALIGN_3

.L25:
	movl	M,  %eax
	andl	$4, %eax
	jle	.L26
	ALIGN_3

	movsd	-32 * SIZE(X), %xmm0
	movhps	-30 * SIZE(X), %xmm0

	mulps	ALPHA, %xmm0

	addps	-32 * SIZE(Y), %xmm0

	movaps	%xmm0, -32 * SIZE(Y)

	addl	$4 * SIZE, X
	addl	$4 * SIZE, Y
	ALIGN_3

.L26:
	movl	M,  %eax
	andl	$2, %eax
	jle	.L27
	ALIGN_3

	movsd	-32 * SIZE(X), %xmm0
	movsd	-32 * SIZE(Y), %xmm4

	mulps	ALPHA, %xmm0
	addps	%xmm4, %xmm0

	movsd	%xmm0, -32 * SIZE(Y)

	addl	$2 * SIZE, X
	addl	$2 * SIZE, Y
	ALIGN_3

.L27:
	movl	M,  %eax
	andl	$1, %eax
	jle	.L29
	ALIGN_3

	movss	-32 * SIZE(X), %xmm0
	mulss	ALPHA, %xmm0
	addss	-32 * SIZE(Y), %xmm0

	movss	%xmm0, -32 * SIZE(Y)
	addl	$SIZE, Y
	ALIGN_3

.L29:
	popl	%ebp
	popl	%ebx
	popl	%esi
	popl	%edi
	ret
#endif
	ALIGN_3


.L50:
	movl	M, %eax
	movl	Y, YY
//If incx==0 || incy==0, avoid unloop.
	cmpl	$0, INCX
	je  .L56
	cmpl	$0, INCY
	je  .L56

	sarl	$3,   %eax
	jle	.L55
	ALIGN_3

.L51:
	movss	(X), %xmm0
	addl	INCX, X
	mulss	ALPHA, %xmm0
	movss	(YY), %xmm6
	addl	INCY, YY
	addss	%xmm6, %xmm0

	movss	(X), %xmm1
	addl	INCX, X
	mulss	ALPHA, %xmm1
	movss	(YY), %xmm6
	addl	INCY, YY
	addss	%xmm6, %xmm1

	movss	(X), %xmm2
	addl	INCX, X
	mulss	ALPHA, %xmm2
	movss	(YY), %xmm6
	addl	INCY, YY
	addss	%xmm6, %xmm2

	movss	(X), %xmm3
	addl	INCX, X
	mulss	ALPHA, %xmm3
	movss	(YY), %xmm6
	addl	INCY, YY
	addss	%xmm6, %xmm3

	movss	%xmm0, (Y)
	addl	INCY, Y
	movss	%xmm1, (Y)
	addl	INCY, Y
	movss	%xmm2, (Y)
	addl	INCY, Y
	movss	%xmm3, (Y)
	addl	INCY, Y

	movss	(X), %xmm0
	addl	INCX, X
	mulss	ALPHA, %xmm0
	movss	(YY), %xmm6
	addl	INCY, YY
	addss	%xmm6, %xmm0

	movss	(X), %xmm1
	addl	INCX, X
	mulss	ALPHA, %xmm1
	movss	(YY), %xmm6
	addl	INCY, YY
	addss	%xmm6, %xmm1

	movss	(X), %xmm2
	addl	INCX, X
	mulss	ALPHA, %xmm2
	movss	(YY), %xmm6
	addl	INCY, YY
	addss	%xmm6, %xmm2

	movss	(X), %xmm3
	addl	INCX, X
	mulss	ALPHA, %xmm3
	movss	(YY), %xmm6
	addl	INCY, YY
	addss	%xmm6, %xmm3

	movss	%xmm0, (Y)
	addl	INCY, Y
	movss	%xmm1, (Y)
	addl	INCY, Y
	movss	%xmm2, (Y)
	addl	INCY, Y
	movss	%xmm3, (Y)
	addl	INCY, Y

	decl	%eax
	jg	.L51
	ALIGN_3

.L55:
	movl	M, %eax
	andl	$7,   %eax
	jle	.L59
	ALIGN_3

.L56:
	movss	(X), %xmm0
	addl	INCX, X
	mulss	ALPHA, %xmm0
	movss	(Y), %xmm6
	addss	%xmm6, %xmm0
	movss	%xmm0, (Y)
	addl	INCY, Y
	decl	%eax
	jg	.L56
	ALIGN_3

.L59:
	popl	%ebp
	popl	%ebx
	popl	%esi
	popl	%edi
	ret

	EPILOGUE
