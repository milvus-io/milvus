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
#define ARGS     0

#define STACK_M	 	 4 + STACK + ARGS(%esp)
#define STACK_X		24 + STACK + ARGS(%esp)
#define STACK_INCX	28 + STACK + ARGS(%esp)
#define STACK_Y		32 + STACK + ARGS(%esp)
#define STACK_INCY	36 + STACK + ARGS(%esp)

#define M	%edx
#define X	%esi
#define Y	%edi
#define INCX	%ebx
#define INCY	%ecx

#include "l1param.h"

	PROLOGUE
	PROFCODE

	pushl	%ebp
	pushl	%edi
	pushl	%esi
	pushl	%ebx

	movl	STACK_M,    M
	movl	STACK_X,    X
	movl	STACK_Y,    Y
	movl	STACK_INCX, INCX
	movl	STACK_INCY, INCY

	leal	(, INCX, SIZE), INCX
	leal	(, INCY, SIZE), INCY

	cmpl	$SIZE, INCX
	jne	.L40
	cmpl	$SIZE, INCY
	jne	.L40

	testl	$SIZE, Y
	je	.L10

	movsd	0 * SIZE(X), %xmm0
	movsd	0 * SIZE(Y), %xmm1

	movsd	%xmm1, 0 * SIZE(X)
	movsd	%xmm0, 0 * SIZE(Y)

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
	ALIGN_3

.L11:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movaps	-16 * SIZE(X), %xmm0
	movaps	-16 * SIZE(Y), %xmm1

	movaps	%xmm0, 	-16 * SIZE(Y)
	movaps	%xmm1, 	-16 * SIZE(X)

	movaps	-14 * SIZE(X), %xmm0
	movaps	-14 * SIZE(Y), %xmm1

	movaps	%xmm0, 	-14 * SIZE(Y)
	movaps	%xmm1, 	-14 * SIZE(X)

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	movaps	-12 * SIZE(X), %xmm0
	movaps	-12 * SIZE(Y), %xmm1

	movaps	%xmm0, 	-12 * SIZE(Y)
	movaps	%xmm1, 	-12 * SIZE(X)

	movaps	-10 * SIZE(X), %xmm0
	movaps	-10 * SIZE(Y), %xmm1

	movaps	%xmm0, 	-10 * SIZE(Y)
	movaps	%xmm1, 	-10 * SIZE(X)

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE + 64) - PREOFFSET(X)
#endif

	movaps	 -8 * SIZE(X), %xmm0
	movaps	 -8 * SIZE(Y), %xmm1

	movaps	%xmm0, 	 -8 * SIZE(Y)
	movaps	%xmm1, 	 -8 * SIZE(X)

	movaps	 -6 * SIZE(X), %xmm0
	movaps	 -6 * SIZE(Y), %xmm1

	movaps	%xmm0, 	 -6 * SIZE(Y)
	movaps	%xmm1, 	 -6 * SIZE(X)

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE + 64) - PREOFFSET(Y)
#endif

	movaps	 -4 * SIZE(X), %xmm0
	movaps	 -4 * SIZE(Y), %xmm1

	movaps	%xmm0, 	 -4 * SIZE(Y)
	movaps	%xmm1, 	 -4 * SIZE(X)

	movaps	 -2 * SIZE(X), %xmm0
	movaps	 -2 * SIZE(Y), %xmm1

	movaps	%xmm0, 	 -2 * SIZE(Y)
	movaps	%xmm1, 	 -2 * SIZE(X)

	subl	$-16 * SIZE, Y
	subl	$-16 * SIZE, X

	decl	%eax
	jg	.L11
	ALIGN_3

.L13:
	testl	$8, M
	jle	.L14

	movaps	-16 * SIZE(X), %xmm0
	movaps	-16 * SIZE(Y), %xmm1

	movaps	%xmm0, 	-16 * SIZE(Y)
	movaps	%xmm1, 	-16 * SIZE(X)

	movaps	-14 * SIZE(X), %xmm0
	movaps	-14 * SIZE(Y), %xmm1

	movaps	%xmm0, 	-14 * SIZE(Y)
	movaps	%xmm1, 	-14 * SIZE(X)

	movaps	-12 * SIZE(X), %xmm0
	movaps	-12 * SIZE(Y), %xmm1

	movaps	%xmm0, 	-12 * SIZE(Y)
	movaps	%xmm1, 	-12 * SIZE(X)

	movaps	-10 * SIZE(X), %xmm0
	movaps	-10 * SIZE(Y), %xmm1

	movaps	%xmm0, 	-10 * SIZE(Y)
 	movaps	%xmm1, 	-10 * SIZE(X)

	addl	$8 * SIZE, X
	addl	$8 * SIZE, Y
	ALIGN_3

.L14:
	testl	$4, M
	jle	.L15

	movaps	-16 * SIZE(X), %xmm0
	movaps	-16 * SIZE(Y), %xmm1

	movaps	%xmm0, 	-16 * SIZE(Y)
	movaps	%xmm1, 	-16 * SIZE(X)

	movaps	-14 * SIZE(X), %xmm0
	movaps	-14 * SIZE(Y), %xmm1

	movaps	%xmm0, 	-14 * SIZE(Y)
	movaps	%xmm1, 	-14 * SIZE(X)

	addl	$4 * SIZE, X
	addl	$4 * SIZE, Y
	ALIGN_3

.L15:
	testl	$2, M
	jle	.L16

	movaps	-16 * SIZE(X), %xmm0
	movaps	-16 * SIZE(Y), %xmm1

	movaps	%xmm0, 	-16 * SIZE(Y)
	movaps	%xmm1, 	-16 * SIZE(X)

	addl	$2 * SIZE, X
	addl	$2 * SIZE, Y
	ALIGN_3

.L16:
	testl	$1, M
	jle	.L19

	movsd	-16 * SIZE(X), %xmm0
	movsd	-16 * SIZE(Y), %xmm1

	movlps	%xmm1, 	-16 * SIZE(X)
	movlps	%xmm0, 	-16 * SIZE(Y)
	ALIGN_3

.L19:
	popl	%ebx
	popl	%esi
	popl	%edi
	popl	%ebp
	ret
	ALIGN_3

.L20:
	movhps	 -16 * SIZE(X), %xmm0
	movaps	 -16 * SIZE(Y), %xmm1

	movlps	%xmm1, -16 * SIZE(X)
	decl	M
	jle	.L29

	movl	M,  %eax
	sarl	$4, %eax
	jle	.L23
	ALIGN_4

.L21:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movaps	-15 * SIZE(X), %xmm2
	movaps	-14 * SIZE(Y), %xmm3

	SHUFPD_1  %xmm2, %xmm0
	movaps	%xmm0, -16 * SIZE(Y)
	SHUFPD_1  %xmm3, %xmm1
	movaps	%xmm1, -15 * SIZE(X)

	movaps	-13 * SIZE(X), %xmm0
	movaps	-12 * SIZE(Y), %xmm1

	SHUFPD_1  %xmm0, %xmm2
	movaps	%xmm2, -14 * SIZE(Y)
	SHUFPD_1  %xmm1, %xmm3
	movaps	%xmm3, -13 * SIZE(X)

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	movaps	-11 * SIZE(X), %xmm2
	movaps	-10 * SIZE(Y), %xmm3

	SHUFPD_1  %xmm2, %xmm0
	movaps	%xmm0, -12 * SIZE(Y)
	SHUFPD_1  %xmm3, %xmm1
	movaps	%xmm1, -11 * SIZE(X)

	movaps	 -9 * SIZE(X), %xmm0
	movaps	 -8 * SIZE(Y), %xmm1

	SHUFPD_1  %xmm0, %xmm2
	movaps	%xmm2, -10 * SIZE(Y)
	SHUFPD_1  %xmm1, %xmm3
	movaps	%xmm3,  -9 * SIZE(X)

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE + 64) - PREOFFSET(X)
#endif

	movaps	 -7 * SIZE(X), %xmm2
	movaps	 -6 * SIZE(Y), %xmm3

	SHUFPD_1  %xmm2, %xmm0
	movaps	%xmm0,  -8 * SIZE(Y)
	SHUFPD_1  %xmm3, %xmm1
	movaps	%xmm1,  -7 * SIZE(X)

	movaps	 -5 * SIZE(X), %xmm0
	movaps	 -4 * SIZE(Y), %xmm1

	SHUFPD_1  %xmm0, %xmm2
	movaps	%xmm2,  -6 * SIZE(Y)
	SHUFPD_1  %xmm1, %xmm3
	movaps	%xmm3,  -5 * SIZE(X)

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE + 64) - PREOFFSET(Y)
#endif

	movaps	 -3 * SIZE(X), %xmm2
	movaps	 -2 * SIZE(Y), %xmm3

	SHUFPD_1  %xmm2, %xmm0
	movaps	%xmm0,  -4 * SIZE(Y)
	SHUFPD_1  %xmm3, %xmm1
	movaps	%xmm1,  -3 * SIZE(X)

	movaps	 -1 * SIZE(X), %xmm0
	movaps	  0 * SIZE(Y), %xmm1

	SHUFPD_1  %xmm0, %xmm2
	movaps	%xmm2,  -2 * SIZE(Y)
	SHUFPD_1  %xmm1, %xmm3
	movaps	%xmm3,  -1 * SIZE(X)

	subl	$-16 * SIZE, X
	subl	$-16 * SIZE, Y

	decl	%eax
	jg	.L21
	ALIGN_3

.L23:
	testl	$8, M
	jle	.L24

	movaps	-15 * SIZE(X), %xmm2
	movaps	-14 * SIZE(Y), %xmm3

	SHUFPD_1  %xmm2, %xmm0
	movaps	%xmm0, -16 * SIZE(Y)
	SHUFPD_1  %xmm3, %xmm1
	movaps	%xmm1, -15 * SIZE(X)

	movaps	-13 * SIZE(X), %xmm0
	movaps	-12 * SIZE(Y), %xmm1

	SHUFPD_1  %xmm0, %xmm2
	movaps	%xmm2, -14 * SIZE(Y)
	SHUFPD_1  %xmm1, %xmm3
	movaps	%xmm3, -13 * SIZE(X)

	movaps	-11 * SIZE(X), %xmm2
	movaps	-10 * SIZE(Y), %xmm3

	SHUFPD_1  %xmm2, %xmm0
	movaps	%xmm0, -12 * SIZE(Y)
	SHUFPD_1  %xmm3, %xmm1
	movaps	%xmm1, -11 * SIZE(X)

	movaps	 -9 * SIZE(X), %xmm0
	movaps	 -8 * SIZE(Y), %xmm1

	SHUFPD_1  %xmm0, %xmm2
	movaps	%xmm2, -10 * SIZE(Y)
	SHUFPD_1  %xmm1, %xmm3
	movaps	%xmm3,  -9 * SIZE(X)

	addl	$8 * SIZE, X
	addl	$8 * SIZE, Y
	ALIGN_3

.L24:
	testl	$4, M
	jle	.L25

	movaps	-15 * SIZE(X), %xmm2
	movaps	-14 * SIZE(Y), %xmm3

	SHUFPD_1  %xmm2, %xmm0
	movaps	%xmm0, -16 * SIZE(Y)
	SHUFPD_1  %xmm3, %xmm1
	movaps	%xmm1, -15 * SIZE(X)

	movaps	-13 * SIZE(X), %xmm0
	movaps	-12 * SIZE(Y), %xmm1

	SHUFPD_1  %xmm0, %xmm2
	movaps	%xmm2, -14 * SIZE(Y)
	SHUFPD_1  %xmm1, %xmm3
	movaps	%xmm3, -13 * SIZE(X)

	addl	$4 * SIZE, X
	addl	$4 * SIZE, Y
	ALIGN_3

.L25:
	testl	$2, M
	jle	.L26

	movaps	-15 * SIZE(X), %xmm2
	movaps	-14 * SIZE(Y), %xmm3

	SHUFPD_1  %xmm3, %xmm1
	movaps	%xmm1, -15 * SIZE(X)
	SHUFPD_1  %xmm2, %xmm0
	movaps	%xmm0, -16 * SIZE(Y)

	movaps	%xmm2, %xmm0
	movaps	%xmm3, %xmm1

	addl	$2 * SIZE, X
	addl	$2 * SIZE, Y
	ALIGN_3

.L26:
	testl	$1, M
	jle	.L29

	movhps	%xmm0, -16 * SIZE(Y)
	movhps	-15 * SIZE(X), %xmm0
	movhps	%xmm1, -15 * SIZE(X)

	addl	$SIZE, X
	addl	$SIZE, Y
	ALIGN_3

.L29:
	movhps	%xmm0, -16 * SIZE(Y)

	popl	%ebx
	popl	%esi
	popl	%edi
	popl	%ebp
	ret
	ALIGN_3

.L40:
	movl	M,  %eax
	sarl	$3, %eax
	jle	.L45
	ALIGN_3

.L41:
	movsd	(X), %xmm0
	movsd	(Y), %xmm1

	movsd	%xmm1, (X)
	addl	INCX, X
	movsd	%xmm0, (Y)
	addl	INCY, Y

	movsd	(X), %xmm0
	movsd	(Y), %xmm1

	movsd	%xmm1, (X)
	addl	INCX, X
	movsd	%xmm0, (Y)
	addl	INCY, Y

	movsd	(X), %xmm0
	movsd	(Y), %xmm1

	movsd	%xmm1, (X)
	addl	INCX, X
	movsd	%xmm0, (Y)
	addl	INCY, Y

	movsd	(X), %xmm0
	movsd	(Y), %xmm1

	movsd	%xmm1, (X)
	addl	INCX, X
	movsd	%xmm0, (Y)
	addl	INCY, Y

	movsd	(X), %xmm0
	movsd	(Y), %xmm1

	movsd	%xmm1, (X)
	addl	INCX, X
	movsd	%xmm0, (Y)
	addl	INCY, Y

	movsd	(X), %xmm0
	movsd	(Y), %xmm1

	movsd	%xmm1, (X)
	addl	INCX, X
	movsd	%xmm0, (Y)
	addl	INCY, Y

	movsd	(X), %xmm0
	movsd	(Y), %xmm1

	movsd	%xmm1, (X)
	addl	INCX, X
	movsd	%xmm0, (Y)
	addl	INCY, Y

	movsd	(X), %xmm0
	movsd	(Y), %xmm1

	movsd	%xmm1, (X)
	addl	INCX, X
	movsd	%xmm0, (Y)
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
	movsd	(Y), %xmm1

	movsd	%xmm1, (X)
	movsd	%xmm0, (Y)

	addl	INCX, X
	addl	INCY, Y
	decl	%eax
	jg	.L46
	ALIGN_3

.L47:
	popl	%ebx
	popl	%esi
	popl	%edi
	popl	%ebp
	ret

	EPILOGUE
