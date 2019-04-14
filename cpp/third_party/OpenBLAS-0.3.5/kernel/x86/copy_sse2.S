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
#define ARGS	 0

#define STACK_M		 4 + STACK + ARGS(%esp)
#define STACK_X		 8 + STACK + ARGS(%esp)
#define STACK_INCX	12 + STACK + ARGS(%esp)
#define STACK_Y		16 + STACK + ARGS(%esp)
#define STACK_INCY	20 + STACK + ARGS(%esp)

#define M	%ebx
#define X	%esi
#define INCX	%ecx
#define Y	%edi
#define INCY	%edx

#include "l1param.h"

#ifdef OPTERON
#define LOAD(OFFSET, ADDR, REG)		xorps	REG, REG; addpd	OFFSET(ADDR), REG
#else
#define LOAD(OFFSET, ADDR, REG)		movaps	OFFSET(ADDR), REG
#endif

	PROLOGUE
	PROFCODE

	pushl	%edi
	pushl	%esi
	pushl	%ebx

	movl	STACK_M,     M
	movl	STACK_X,     X
	movl	STACK_INCX,  INCX
	movl	STACK_Y,     Y
	movl	STACK_INCY,  INCY

	leal	(, INCX, SIZE), INCX
	leal	(, INCY, SIZE), INCY

	cmpl	$SIZE, INCX
	jne	.L40
	cmpl	$SIZE, INCY
	jne	.L40

#ifdef ALIGNED_ACCESS
	testl	$SIZE, Y
#else
	testl	$SIZE, X
#endif
	je	.L10

	movsd	(X), %xmm0
	movsd	%xmm0, (Y)
	addl	$1 * SIZE, X
	addl	$1 * SIZE, Y
	decl	M
	jle	.L19
	ALIGN_4

.L10:
	subl	$-16 * SIZE, X
	subl	$-16 * SIZE, Y

#ifdef ALIGNED_ACCESS
	testl	$SIZE, X
#else
	testl	$SIZE, Y
#endif
	jne	.L20

	movl	M,  %eax
	sarl	$4, %eax
	jle	.L13

	movaps	-16 * SIZE(X), %xmm0
	movaps	-14 * SIZE(X), %xmm1
	movaps	-12 * SIZE(X), %xmm2
	movaps	-10 * SIZE(X), %xmm3
	movaps	 -8 * SIZE(X), %xmm4
	movaps	 -6 * SIZE(X), %xmm5
	movaps	 -4 * SIZE(X), %xmm6
	movaps	 -2 * SIZE(X), %xmm7

	decl	%eax
	jle .L12
	ALIGN_3

.L11:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	movaps	%xmm0, -16 * SIZE(Y)
	LOAD( 0 * SIZE, X, %xmm0)
	movaps	%xmm1, -14 * SIZE(Y)
	LOAD( 2 * SIZE, X, %xmm1)

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movaps	%xmm2, -12 * SIZE(Y)
	LOAD( 4 * SIZE, X, %xmm2)
	movaps	%xmm3, -10 * SIZE(Y)
	LOAD( 6 * SIZE, X, %xmm3)

#if defined(PREFETCHW) && !defined(FETCH128)
	PREFETCHW (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	movaps	%xmm4, -8 * SIZE(Y)
	LOAD( 8 * SIZE, X, %xmm4)
	movaps	%xmm5, -6 * SIZE(Y)
	LOAD(10 * SIZE, X, %xmm5)

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	movaps	%xmm6, -4 * SIZE(Y)
	LOAD(12 * SIZE, X, %xmm6)
	movaps	%xmm7, -2 * SIZE(Y)
	LOAD(14 * SIZE, X, %xmm7)

	subl	$-16 * SIZE, Y
	subl	$-16 * SIZE, X
	decl	%eax
	jg	.L11
	ALIGN_3

.L12:
	movaps	%xmm0, -16 * SIZE(Y)
	movaps	%xmm1, -14 * SIZE(Y)
	movaps	%xmm2, -12 * SIZE(Y)
	movaps	%xmm3, -10 * SIZE(Y)
	movaps	%xmm4,  -8 * SIZE(Y)
	movaps	%xmm5,  -6 * SIZE(Y)
	movaps	%xmm6,  -4 * SIZE(Y)
	movaps	%xmm7,  -2 * SIZE(Y)

	subl	$-16 * SIZE, Y
	subl	$-16 * SIZE, X
	ALIGN_3

.L13:
	testl	$8, M
	jle	.L14
	ALIGN_3

	movaps	-16 * SIZE(X), %xmm0
	movaps	-14 * SIZE(X), %xmm1
	movaps	-12 * SIZE(X), %xmm2
	movaps	-10 * SIZE(X), %xmm3

	movaps	%xmm0, -16 * SIZE(Y)
	movaps	%xmm1, -14 * SIZE(Y)
	movaps	%xmm2, -12 * SIZE(Y)
	movaps	%xmm3, -10 * SIZE(Y)

	addl	$8 * SIZE, X
	addl	$8 * SIZE, Y
	ALIGN_3

.L14:
	testl	$4, M
	jle	.L15
	ALIGN_3

	movaps	-16 * SIZE(X), %xmm0
	movaps	-14 * SIZE(X), %xmm1

	movaps	%xmm0, -16 * SIZE(Y)
	movaps	%xmm1, -14 * SIZE(Y)

	addl	$4 * SIZE, X
	addl	$4 * SIZE, Y
	ALIGN_3

.L15:
	testl	$2, M
	jle	.L16
	ALIGN_3

	movaps	-16 * SIZE(X), %xmm0
	movaps	%xmm0, -16 * SIZE(Y)

	addl	$2 * SIZE, X
	addl	$2 * SIZE, Y
	ALIGN_3

.L16:
	testl	$1, M
	jle	.L19
	ALIGN_3

	movsd	-16 * SIZE(X), %xmm0
	movsd	%xmm0, 	-16 * SIZE(Y)
	ALIGN_3

.L19:
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
	movaps	 -9 * SIZE(X), %xmm4
	movaps	 -7 * SIZE(X), %xmm5
	movaps	 -5 * SIZE(X), %xmm6
	movaps	 -3 * SIZE(X), %xmm7

	decl	%eax
	jle .L22
	ALIGN_4

.L21:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	SHUFPD_1 %xmm1, %xmm0
	movaps	%xmm0, -16 * SIZE(Y)
	LOAD(-1 * SIZE, X, %xmm0)

	SHUFPD_1 %xmm2, %xmm1
	movaps	%xmm1, -14 * SIZE(Y)
	LOAD( 1 * SIZE, X, %xmm1)

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	SHUFPD_1 %xmm3, %xmm2
	movaps	%xmm2, -12 * SIZE(Y)
	LOAD( 3 * SIZE, X, %xmm2)

	SHUFPD_1 %xmm4, %xmm3
	movaps	%xmm3, -10 * SIZE(Y)
	LOAD( 5 * SIZE, X, %xmm3)

#if defined(PREFETCHW) && !defined(FETCH128)
	PREFETCHW (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	SHUFPD_1 %xmm5, %xmm4
	movaps	%xmm4,  -8 * SIZE(Y)
	LOAD( 7 * SIZE, X, %xmm4)

	SHUFPD_1 %xmm6, %xmm5
	movaps	%xmm5, -6 * SIZE(Y)
	LOAD( 9 * SIZE, X, %xmm5)

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	SHUFPD_1 %xmm7, %xmm6
	movaps	%xmm6, -4 * SIZE(Y)
	LOAD(11 * SIZE, X, %xmm6)

	SHUFPD_1 %xmm0, %xmm7
	movaps	%xmm7, -2 * SIZE(Y)
	LOAD(13 * SIZE, X, %xmm7)

	subl	$-16 * SIZE, X
	subl	$-16 * SIZE, Y
	decl	%eax
	jg	.L21
	ALIGN_3

.L22:
	SHUFPD_1 %xmm1, %xmm0
	movaps	%xmm0, -16 * SIZE(Y)
	LOAD(-1 * SIZE, X, %xmm0)

	SHUFPD_1 %xmm2, %xmm1
	movaps	%xmm1, -14 * SIZE(Y)

	SHUFPD_1 %xmm3, %xmm2
	movaps	%xmm2, -12 * SIZE(Y)
	SHUFPD_1 %xmm4, %xmm3
	movaps	%xmm3, -10 * SIZE(Y)

	SHUFPD_1 %xmm5, %xmm4
	movaps	%xmm4,  -8 * SIZE(Y)
	SHUFPD_1 %xmm6, %xmm5
	movaps	%xmm5,  -6 * SIZE(Y)

	SHUFPD_1 %xmm7, %xmm6
	movaps	%xmm6,  -4 * SIZE(Y)
	SHUFPD_1 %xmm0, %xmm7
	movaps	%xmm7,  -2 * SIZE(Y)

	subl	$-16 * SIZE, X
	subl	$-16 * SIZE, Y
	ALIGN_3

.L23:
	testl	$8, M
	jle	.L24
	ALIGN_3

	movaps	-15 * SIZE(X), %xmm1
	movaps	-13 * SIZE(X), %xmm2
	movaps	-11 * SIZE(X), %xmm3
	movaps	 -9 * SIZE(X), %xmm4

	SHUFPD_1 %xmm1, %xmm0
	movaps	%xmm0, -16 * SIZE(Y)

	SHUFPD_1 %xmm2, %xmm1
	movaps	%xmm1, -14 * SIZE(Y)

	SHUFPD_1 %xmm3, %xmm2
	movaps	%xmm2, -12 * SIZE(Y)

	SHUFPD_1 %xmm4, %xmm3
	movaps	%xmm3, -10 * SIZE(Y)

	movaps	%xmm4, %xmm0

	addl	$8 * SIZE, X
	addl	$8 * SIZE, Y
	ALIGN_3

.L24:
	testl	$4, M
	jle	.L25
	ALIGN_3

	movaps	-15 * SIZE(X), %xmm1
	movaps	-13 * SIZE(X), %xmm2

	SHUFPD_1 %xmm1, %xmm0
	SHUFPD_1 %xmm2, %xmm1

	movaps	%xmm0, -16 * SIZE(Y)
	movaps	%xmm1, -14 * SIZE(Y)
	movaps	%xmm2, %xmm0

	addl	$4 * SIZE, X
	addl	$4 * SIZE, Y
	ALIGN_3

.L25:
	testl	$2, M
	jle	.L26
	ALIGN_3

	movaps	-15 * SIZE(X), %xmm1
	SHUFPD_1 %xmm1, %xmm0

	movaps	%xmm0, -16 * SIZE(Y)

	addl	$2 * SIZE, X
	addl	$2 * SIZE, Y
	ALIGN_3

.L26:
	testl	$1, M
	jle	.L29
	ALIGN_3

	movsd	-16 * SIZE(X), %xmm0
	movsd	%xmm0, 	-16 * SIZE(Y)
	ALIGN_3

.L29:
	popl	%ebx
	popl	%esi
	popl	%edi
	ret
	ALIGN_3

#else

	movl	M,  %eax
	sarl	$4, %eax
	jle	.L23

	movaps	-16 * SIZE(X), %xmm0
	movaps	-14 * SIZE(X), %xmm1
	movaps	-12 * SIZE(X), %xmm2
	movaps	-10 * SIZE(X), %xmm3
	movaps	 -8 * SIZE(X), %xmm4
	movaps	 -6 * SIZE(X), %xmm5
	movaps	 -4 * SIZE(X), %xmm6
	movaps	 -2 * SIZE(X), %xmm7

	decl	%eax
	jle .L22
	ALIGN_3

.L21:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	movlps	%xmm0, -16 * SIZE(Y)
	movhps	%xmm0, -15 * SIZE(Y)
	LOAD( 0 * SIZE, X, %xmm0)
	movlps	%xmm1, -14 * SIZE(Y)
	movhps	%xmm1, -13 * SIZE(Y)
	LOAD( 2 * SIZE, X, %xmm1)

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movlps	%xmm2, -12 * SIZE(Y)
	movhps	%xmm2, -11 * SIZE(Y)
	LOAD( 4 * SIZE, X, %xmm2)
	movlps	%xmm3, -10 * SIZE(Y)
	movhps	%xmm3,  -9 * SIZE(Y)
	LOAD( 6 * SIZE, X, %xmm3)

#if defined(PREFETCHW) && !defined(FETCH128)
	PREFETCHW (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	movlps	%xmm4,  -8 * SIZE(Y)
	movhps	%xmm4,  -7 * SIZE(Y)
	LOAD( 8 * SIZE, X, %xmm4)
	movlps	%xmm5,  -6 * SIZE(Y)
	movhps	%xmm5,  -5 * SIZE(Y)
	LOAD(10 * SIZE, X, %xmm5)

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	movlps	%xmm6,  -4 * SIZE(Y)
	movhps	%xmm6,  -3 * SIZE(Y)
	LOAD(12 * SIZE, X, %xmm6)
	movlps	%xmm7,  -2 * SIZE(Y)
	movhps	%xmm7,  -1 * SIZE(Y)
	LOAD(14 * SIZE, X, %xmm7)

	subl	$-16 * SIZE, Y
	subl	$-16 * SIZE, X
	decl	%eax
	jg	.L21
	ALIGN_3

.L22:
	movlps	%xmm0, -16 * SIZE(Y)
	movhps	%xmm0, -15 * SIZE(Y)
	movlps	%xmm1, -14 * SIZE(Y)
	movhps	%xmm1, -13 * SIZE(Y)
	movlps	%xmm2, -12 * SIZE(Y)
	movhps	%xmm2, -11 * SIZE(Y)
	movlps	%xmm3, -10 * SIZE(Y)
	movhps	%xmm3,  -9 * SIZE(Y)
	movlps	%xmm4,  -8 * SIZE(Y)
	movhps	%xmm4,  -7 * SIZE(Y)
	movlps	%xmm5,  -6 * SIZE(Y)
	movhps	%xmm5,  -5 * SIZE(Y)
	movlps	%xmm6,  -4 * SIZE(Y)
	movhps	%xmm6,  -3 * SIZE(Y)
	movlps	%xmm7,  -2 * SIZE(Y)
	movhps	%xmm7,  -1 * SIZE(Y)

	subl	$-16 * SIZE, Y
	subl	$-16 * SIZE, X
	ALIGN_3

.L23:
	testl	$8, M
	jle	.L24
	ALIGN_3

	movaps	-16 * SIZE(X), %xmm0
	movlps	%xmm0, -16 * SIZE(Y)
	movhps	%xmm0, -15 * SIZE(Y)
	movaps	-14 * SIZE(X), %xmm1
	movlps	%xmm1, -14 * SIZE(Y)
	movhps	%xmm1, -13 * SIZE(Y)
	movaps	-12 * SIZE(X), %xmm2
	movlps	%xmm2, -12 * SIZE(Y)
	movhps	%xmm2, -11 * SIZE(Y)
	movaps	-10 * SIZE(X), %xmm3
	movlps	%xmm3, -10 * SIZE(Y)
	movhps	%xmm3,  -9 * SIZE(Y)

	addl	$8 * SIZE, X
	addl	$8 * SIZE, Y
	ALIGN_3

.L24:
	testl	$4, M
	jle	.L25
	ALIGN_3

	movaps	-16 * SIZE(X), %xmm0
	movlps	%xmm0, -16 * SIZE(Y)
	movhps	%xmm0, -15 * SIZE(Y)
	movaps	-14 * SIZE(X), %xmm1
	movlps	%xmm1, -14 * SIZE(Y)
	movhps	%xmm1, -13 * SIZE(Y)

	addl	$4 * SIZE, X
	addl	$4 * SIZE, Y
	ALIGN_3

.L25:
	testl	$2, M
	jle	.L26
	ALIGN_3

	movaps	-16 * SIZE(X), %xmm0
	movlps	%xmm0, -16 * SIZE(Y)
	movhps	%xmm0, -15 * SIZE(Y)

	addl	$2 * SIZE, X
	addl	$2 * SIZE, Y
	ALIGN_3

.L26:
	testl	$1, M
	jle	.L29
	ALIGN_3

	movsd	-16 * SIZE(X), %xmm0
	movsd	%xmm0, 	-16 * SIZE(Y)
	ALIGN_3

.L29:
	popl	%ebx
	popl	%esi
	popl	%edi
	ret
	ALIGN_3

#endif

.L40:
	movl	M,  %eax
	sarl	$3, %eax
	jle	.L45
	ALIGN_3

.L41:
	movsd	(X), %xmm0
	addl	INCX, X
	movhps	(X), %xmm0
	addl	INCX, X
	movsd	(X), %xmm1
	addl	INCX, X
	movhps	(X), %xmm1
	addl	INCX, X
	movsd	(X), %xmm2
	addl	INCX, X
	movhps	(X), %xmm2
	addl	INCX, X
	movsd	(X), %xmm3
	addl	INCX, X
	movhps	(X), %xmm3
	addl	INCX, X

	movlps	%xmm0, (Y)
	addl	INCY, Y
	movhps	%xmm0, (Y)
	addl	INCY, Y
	movlps	%xmm1, (Y)
	addl	INCY, Y
	movhps	%xmm1, (Y)
	addl	INCY, Y
	movlps	%xmm2, (Y)
	addl	INCY, Y
	movhps	%xmm2, (Y)
	addl	INCY, Y
	movlps	%xmm3, (Y)
	addl	INCY, Y
	movhps	%xmm3, (Y)
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
	movlps	%xmm0, (Y)
	addl	INCY, Y
	decl	%eax
	jg	.L46
	ALIGN_3

.L47:
	popl	%ebx
	popl	%esi
	popl	%edi
	ret

	EPILOGUE
