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

#define N	%ecx
#define X	%esi
#define INCX	%ebx
#define Y	%edi
#define INCY	%edx

#include "l1param.h"

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

	leal	(, INCX, SIZE), INCX
	leal	(, INCY, SIZE), INCY

	xorps	%xmm0, %xmm0
	xorps	%xmm1, %xmm1
	xorps	%xmm2, %xmm2
	xorps	%xmm3, %xmm3

	cmpl	$0, N
	jle	.L999

	cmpl	$SIZE, INCX
	jne	.L50
	cmpl	$SIZE, INCY
	jne	.L50

	subl	$-16 * SIZE, X
	subl	$-16 * SIZE, Y

	testl	$SIZE, Y
	je	.L10

	movsd	-16 * SIZE(X), %xmm0
	mulsd	-16 * SIZE(Y), %xmm0
	addl	$1 * SIZE, X
	addl	$1 * SIZE, Y
	decl	N
	ALIGN_2

.L10:
	testl	$SIZE, X
	jne	.L20

	movl	N,  %eax
	sarl	$4, %eax
	jle	.L14

	movaps	-16 * SIZE(X), %xmm4
	movaps	-14 * SIZE(X), %xmm5
	movaps	-12 * SIZE(X), %xmm6
	movaps	-10 * SIZE(X), %xmm7

	decl	%eax
	jle	.L12
	ALIGN_3

.L11:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	mulpd	-16 * SIZE(Y), %xmm4
	addpd	 %xmm4, %xmm0
	movaps	 -8 * SIZE(X), %xmm4

	mulpd	-14 * SIZE(Y), %xmm5
	addpd	 %xmm5, %xmm1
	movaps	 -6 * SIZE(X), %xmm5

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	mulpd	-12 * SIZE(Y), %xmm6
	addpd	 %xmm6, %xmm2
	movaps	 -4 * SIZE(X), %xmm6

	mulpd	-10 * SIZE(Y), %xmm7
	addpd	 %xmm7, %xmm3
	movaps	 -2 * SIZE(X), %xmm7

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	mulpd	 -8 * SIZE(Y), %xmm4
	addpd	 %xmm4, %xmm0
	movaps	  0 * SIZE(X), %xmm4

	mulpd	 -6 * SIZE(Y), %xmm5
	addpd	 %xmm5, %xmm1
	movaps	  2 * SIZE(X), %xmm5

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	mulpd	 -4 * SIZE(Y), %xmm6
	addpd	 %xmm6, %xmm2
	movaps	  4 * SIZE(X), %xmm6

	mulpd	 -2 * SIZE(Y), %xmm7
	addpd	 %xmm7, %xmm3
	movaps	  6 * SIZE(X), %xmm7

	subl	$-16 * SIZE, X
	subl	$-16 * SIZE, Y

	decl	%eax
	jg	.L11
	ALIGN_3

.L12:
	mulpd	-16 * SIZE(Y), %xmm4
	addpd	 %xmm4, %xmm0
	movaps	 -8 * SIZE(X), %xmm4

	mulpd	-14 * SIZE(Y), %xmm5
	addpd	 %xmm5, %xmm1
	movaps	 -6 * SIZE(X), %xmm5

	mulpd	-12 * SIZE(Y), %xmm6
	addpd	 %xmm6, %xmm2
	movaps	 -4 * SIZE(X), %xmm6

	mulpd	-10 * SIZE(Y), %xmm7
	addpd	 %xmm7, %xmm3
	movaps	 -2 * SIZE(X), %xmm7

	mulpd	 -8 * SIZE(Y), %xmm4
	addpd	 %xmm4, %xmm0
	mulpd	 -6 * SIZE(Y), %xmm5
	addpd	 %xmm5, %xmm1
	mulpd	 -4 * SIZE(Y), %xmm6
	addpd	 %xmm6, %xmm2
	mulpd	 -2 * SIZE(Y), %xmm7
	addpd	 %xmm7, %xmm3

	subl	$-16 * SIZE, X
	subl	$-16 * SIZE, Y
	ALIGN_3

.L14:
	testl	$15, N
	jle	.L999

	testl	$8, N
	jle	.L15

	movaps	-16 * SIZE(X), %xmm4
	movaps	-14 * SIZE(X), %xmm5
	movaps	-12 * SIZE(X), %xmm6
	movaps	-10 * SIZE(X), %xmm7

	mulpd	-16 * SIZE(Y), %xmm4
	addpd	 %xmm4,  %xmm0
	mulpd	-14 * SIZE(Y), %xmm5
	addpd	 %xmm5,  %xmm1
	mulpd	-12 * SIZE(Y), %xmm6
	addpd	 %xmm6,  %xmm2
	mulpd	-10 * SIZE(Y), %xmm7
	addpd	 %xmm7,  %xmm3

	addl	$8 * SIZE, X
	addl	$8 * SIZE, Y
	ALIGN_3

.L15:
	testl	$4, N
	jle	.L16

	movaps	-16 * SIZE(X), %xmm4
	movaps	-14 * SIZE(X), %xmm5

	mulpd	-16 * SIZE(Y), %xmm4
	addpd	 %xmm4,  %xmm0
	mulpd	-14 * SIZE(Y), %xmm5
	addpd	 %xmm5,  %xmm1

	addl	$4 * SIZE, X
	addl	$4 * SIZE, Y
	ALIGN_3

.L16:
	testl	$2, N
	jle	.L17

	movaps	-16 * SIZE(X), %xmm4

	mulpd	-16 * SIZE(Y), %xmm4
	addpd	 %xmm4,  %xmm0

	addl	$2 * SIZE, X
	addl	$2 * SIZE, Y
	ALIGN_3

.L17:
	testl	$1, N
	jle	.L999

	movsd	-16 * SIZE(X), %xmm4
	mulsd	-16 * SIZE(Y), %xmm4
	addsd	%xmm4,  %xmm0
	jmp	.L999
	ALIGN_3

.L20:

#ifdef ALIGNED_ACCESS

	movhps	-16 * SIZE(X), %xmm4
	addl	$SIZE, X

	movl	N,  %eax
	sarl	$4, %eax
	jle	.L24

	movaps	-16 * SIZE(X), %xmm5
	movaps	-14 * SIZE(X), %xmm6
	movaps	-12 * SIZE(X), %xmm7

	decl	%eax
	jle	.L22

	ALIGN_3

.L21:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	SHUFPD_1 %xmm5, %xmm4
	mulpd	-16 * SIZE(Y), %xmm4
	addpd	 %xmm4,  %xmm0
	movaps	-10 * SIZE(X), %xmm4

	SHUFPD_1 %xmm6, %xmm5
	mulpd	-14 * SIZE(Y), %xmm5
	addpd	 %xmm5,  %xmm1
	movaps	 -8 * SIZE(X), %xmm5

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	SHUFPD_1 %xmm7, %xmm6
	mulpd	-12 * SIZE(Y), %xmm6
	addpd	 %xmm6,  %xmm2
	movaps	 -6 * SIZE(X), %xmm6

	SHUFPD_1 %xmm4, %xmm7
	mulpd	-10 * SIZE(Y), %xmm7
	addpd	 %xmm7,  %xmm3
	movaps	 -4 * SIZE(X), %xmm7

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	SHUFPD_1 %xmm5, %xmm4
	mulpd	 -8 * SIZE(Y), %xmm4
	addpd	 %xmm4,  %xmm0
	movaps	 -2 * SIZE(X), %xmm4

	SHUFPD_1 %xmm6, %xmm5
	mulpd	 -6 * SIZE(Y), %xmm5
	addpd	 %xmm5,  %xmm1
	movaps	  0 * SIZE(X), %xmm5

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	SHUFPD_1 %xmm7, %xmm6
	mulpd	 -4 * SIZE(Y), %xmm6
	addpd	 %xmm6, %xmm2
	movaps	  2 * SIZE(X), %xmm6

	SHUFPD_1 %xmm4, %xmm7
	mulpd	 -2 * SIZE(Y), %xmm7
	addpd	 %xmm7, %xmm3
	movaps	  4 * SIZE(X), %xmm7

	subl	$-16 * SIZE, X
	subl	$-16 * SIZE, Y

	decl	%eax
	jg	.L21
	ALIGN_3

.L22:
	SHUFPD_1 %xmm5, %xmm4
	mulpd	-16 * SIZE(Y), %xmm4
	addpd	 %xmm4,  %xmm0
	movaps	-10 * SIZE(X), %xmm4

	SHUFPD_1 %xmm6, %xmm5
	mulpd	-14 * SIZE(Y), %xmm5
	addpd	 %xmm5,  %xmm1
	movaps	 -8 * SIZE(X), %xmm5

	SHUFPD_1 %xmm7, %xmm6
	mulpd	-12 * SIZE(Y), %xmm6
	addpd	 %xmm6,  %xmm2
	movaps	 -6 * SIZE(X), %xmm6

	SHUFPD_1 %xmm4, %xmm7
	mulpd	-10 * SIZE(Y), %xmm7
	addpd	 %xmm7,  %xmm3
	movaps	 -4 * SIZE(X), %xmm7

	SHUFPD_1 %xmm5, %xmm4
	mulpd	 -8 * SIZE(Y), %xmm4
	addpd	 %xmm4,  %xmm0
	movaps	 -2 * SIZE(X), %xmm4

	SHUFPD_1 %xmm6, %xmm5
	mulpd	 -6 * SIZE(Y), %xmm5
	addpd	 %xmm5,  %xmm1

	SHUFPD_1 %xmm7, %xmm6
	mulpd	 -4 * SIZE(Y), %xmm6
	addpd	 %xmm6, %xmm2

	SHUFPD_1 %xmm4, %xmm7
	mulpd	 -2 * SIZE(Y), %xmm7
	addpd	 %xmm7, %xmm3

	subl	$-16 * SIZE, X
	subl	$-16 * SIZE, Y
	ALIGN_3

.L24:
	testl	$15, N
	jle	.L999

	testl	$8, N
	jle	.L25

	movaps	-16 * SIZE(X), %xmm5
	movaps	-14 * SIZE(X), %xmm6
	movaps	-12 * SIZE(X), %xmm7

	SHUFPD_1 %xmm5, %xmm4
	mulpd	-16 * SIZE(Y), %xmm4
	addpd	 %xmm4,  %xmm0
	movaps	-10 * SIZE(X), %xmm4

	SHUFPD_1 %xmm6, %xmm5
	mulpd	-14 * SIZE(Y), %xmm5
	addpd	 %xmm5,  %xmm1

	SHUFPD_1 %xmm7, %xmm6
	mulpd	-12 * SIZE(Y), %xmm6
	addpd	 %xmm6,  %xmm2

	SHUFPD_1 %xmm4, %xmm7
	mulpd	-10 * SIZE(Y), %xmm7
	addpd	 %xmm7,  %xmm3

	addl	$8 * SIZE, X
	addl	$8 * SIZE, Y
	ALIGN_3

.L25:
	testl	$4, N
	jle	.L26

	movaps	-16 * SIZE(X), %xmm5
	movaps	-14 * SIZE(X), %xmm6

	SHUFPD_1 %xmm5, %xmm4
	mulpd	-16 * SIZE(Y), %xmm4
	addpd	 %xmm4, %xmm0
	SHUFPD_1 %xmm6, %xmm5
	mulpd	-14 * SIZE(Y), %xmm5
	addpd	 %xmm5, %xmm1
	movapd	 %xmm6, %xmm4

	addl	$4 * SIZE, X
	addl	$4 * SIZE, Y
	ALIGN_3

.L26:
	testl	$2, N
	jle	.L27

	movaps	-16 * SIZE(X), %xmm5

	SHUFPD_1 %xmm5, %xmm4
	mulpd	-16 * SIZE(Y), %xmm4
	addpd	 %xmm4,  %xmm0
	movapd	 %xmm5, %xmm4

	addl	$2 * SIZE, X
	addl	$2 * SIZE, Y
	ALIGN_3

.L27:
	testl	$1, N
	jle	.L999

	SHUFPD_1 %xmm4, %xmm4
	mulsd	-16 * SIZE(Y), %xmm4
	addsd	%xmm4,  %xmm0
	jmp	.L999
	ALIGN_3

#else

	movl	N,  %eax
	sarl	$4, %eax
	jle	.L24

	movlps	-16 * SIZE(X), %xmm4
	movhps	-15 * SIZE(X), %xmm4
	movlps	-14 * SIZE(X), %xmm5
	movhps	-13 * SIZE(X), %xmm5
	movlps	-12 * SIZE(X), %xmm6
	movhps	-11 * SIZE(X), %xmm6
	movlps	-10 * SIZE(X), %xmm7
	movhps	 -9 * SIZE(X), %xmm7

	decl	%eax
	jle	.L22

	ALIGN_3

.L21:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	mulpd	-16 * SIZE(Y), %xmm4
	addpd	 %xmm4,  %xmm0
	movlps	 -8 * SIZE(X), %xmm4
	movhps	 -7 * SIZE(X), %xmm4

	mulpd	-14 * SIZE(Y), %xmm5
	addpd	 %xmm5,  %xmm1
	movlps	 -6 * SIZE(X), %xmm5
	movhps	 -5 * SIZE(X), %xmm5

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	mulpd	-12 * SIZE(Y), %xmm6
	addpd	 %xmm6,  %xmm2
	movlps	 -4 * SIZE(X), %xmm6
	movhps	 -3 * SIZE(X), %xmm6

	mulpd	-10 * SIZE(Y), %xmm7
	addpd	 %xmm7,  %xmm3
	movlps	 -2 * SIZE(X), %xmm7
	movhps	 -1 * SIZE(X), %xmm7

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	mulpd	 -8 * SIZE(Y), %xmm4
	addpd	 %xmm4,  %xmm0
	movlps	  0 * SIZE(X), %xmm4
	movhps	  1 * SIZE(X), %xmm4

	mulpd	 -6 * SIZE(Y), %xmm5
	addpd	 %xmm5,  %xmm1
	movlps	  2 * SIZE(X), %xmm5
	movhps	  3 * SIZE(X), %xmm5

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	mulpd	 -4 * SIZE(Y), %xmm6
	addpd	 %xmm6,  %xmm2
	movlps	  4 * SIZE(X), %xmm6
	movhps	  5 * SIZE(X), %xmm6

	mulpd	 -2 * SIZE(Y), %xmm7
	addpd	 %xmm7,  %xmm3
	movlps	  6 * SIZE(X), %xmm7
	movhps	  7 * SIZE(X), %xmm7

	subl	$-16 * SIZE, X
	subl	$-16 * SIZE, Y

	decl	%eax
	jg	.L21
	ALIGN_3

.L22:
	mulpd	-16 * SIZE(Y), %xmm4
	addpd	 %xmm4,  %xmm0
	movlps	 -8 * SIZE(X), %xmm4
	movhps	 -7 * SIZE(X), %xmm4

	mulpd	-14 * SIZE(Y), %xmm5
	addpd	 %xmm5,  %xmm1
	movlps	 -6 * SIZE(X), %xmm5
	movhps	 -5 * SIZE(X), %xmm5

	mulpd	-12 * SIZE(Y), %xmm6
	addpd	 %xmm6,  %xmm2
	movlps	 -4 * SIZE(X), %xmm6
	movhps	 -3 * SIZE(X), %xmm6

	mulpd	-10 * SIZE(Y), %xmm7
	addpd	 %xmm7,  %xmm3
	movlps	 -2 * SIZE(X), %xmm7
	movhps	 -1 * SIZE(X), %xmm7

	mulpd	 -8 * SIZE(Y), %xmm4
	addpd	 %xmm4,  %xmm0
	mulpd	 -6 * SIZE(Y), %xmm5
	addpd	 %xmm5,  %xmm1
	mulpd	 -4 * SIZE(Y), %xmm6
	addpd	 %xmm6,  %xmm2
	mulpd	 -2 * SIZE(Y), %xmm7
	addpd	 %xmm7,  %xmm3

	subl	$-16 * SIZE, X
	subl	$-16 * SIZE, Y
	ALIGN_3

.L24:
	testl	$15, N
	jle	.L999

	testl	$8, N
	jle	.L25

	movlps	-16 * SIZE(X), %xmm4
	movhps	-15 * SIZE(X), %xmm4
	movlps	-14 * SIZE(X), %xmm5
	movhps	-13 * SIZE(X), %xmm5
	movlps	-12 * SIZE(X), %xmm6
	movhps	-11 * SIZE(X), %xmm6
	movlps	-10 * SIZE(X), %xmm7
	movhps	 -9 * SIZE(X), %xmm7

	mulpd	-16 * SIZE(Y), %xmm4
	addpd	 %xmm4,  %xmm0
	mulpd	-14 * SIZE(Y), %xmm5
	addpd	 %xmm5,  %xmm1
	mulpd	-12 * SIZE(Y), %xmm6
	addpd	 %xmm6,  %xmm2
	mulpd	-10 * SIZE(Y), %xmm7
	addpd	 %xmm7,  %xmm3

	addl	$8 * SIZE, X
	addl	$8 * SIZE, Y
	ALIGN_3

.L25:
	testl	$4, N
	jle	.L26

	movlps	-16 * SIZE(X), %xmm4
	movhps	-15 * SIZE(X), %xmm4
	movlps	-14 * SIZE(X), %xmm5
	movhps	-13 * SIZE(X), %xmm5

	mulpd	-16 * SIZE(Y), %xmm4
	addpd	 %xmm4,  %xmm0
	mulpd	-14 * SIZE(Y), %xmm5
	addpd	 %xmm5,  %xmm1

	addl	$4 * SIZE, X
	addl	$4 * SIZE, Y
	ALIGN_3

.L26:
	testl	$2, N
	jle	.L27

	movlps	-16 * SIZE(X), %xmm4
	movhps	-15 * SIZE(X), %xmm4

	mulpd	-16 * SIZE(Y), %xmm4
	addpd	 %xmm4,  %xmm0

	addl	$2 * SIZE, X
	addl	$2 * SIZE, Y
	ALIGN_3

.L27:
	testl	$1, N
	jle	.L999

	movsd	-16 * SIZE(X), %xmm4
	mulsd	-16 * SIZE(Y), %xmm4
	addsd	%xmm4,  %xmm0
	jmp	.L999
	ALIGN_3
#endif

.L50:
	movl	N,  %eax
	sarl	$2, %eax
	jle	.L55
	ALIGN_3

.L53:
	movsd	(X), %xmm4
	addl	INCX, X
	mulsd	(Y), %xmm4
	addl	INCY, Y
	movsd	(X), %xmm5
	addl	INCX, X
	mulsd	(Y), %xmm5
	addl	INCY, Y
	movsd	(X), %xmm6
	addl	INCX, X
	mulsd	(Y), %xmm6
	addl	INCY, Y
	movsd	(X), %xmm7
	addl	INCX, X
	mulsd	(Y), %xmm7
	addl	INCY, Y

	addsd	%xmm4, %xmm0
	addsd	%xmm5, %xmm1
	addsd	%xmm6, %xmm2
	addsd	%xmm7, %xmm3

	decl	%eax
	jg	.L53
	ALIGN_3

.L55:
	movl	N,  %eax
	andl	$3, %eax
	jle	.L999
	ALIGN_3

.L56:
	movsd	(X), %xmm4
	addl	INCX, X
	mulsd	(Y), %xmm4
	addl	INCY, Y
	addsd	%xmm4, %xmm0
	decl	%eax
	jg	.L56
	ALIGN_3

.L999:
	addpd	%xmm1, %xmm0
	addpd	%xmm3, %xmm2
	addpd	%xmm2, %xmm0

#ifndef HAVE_SSE3
	pshufd	$0xe, %xmm0, %xmm1
	addsd	%xmm1, %xmm0
#else
	haddpd	%xmm0, %xmm0
#endif
	movlps	%xmm0, STACK_N
	fldl	STACK_N

	popl	%ebx
	popl	%esi
	popl	%edi
	ret

	EPILOGUE
