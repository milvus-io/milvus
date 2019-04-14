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
	movaps	%xmm3,  ALPHA

	movq	40(%rsp), X
	movq	48(%rsp), INCX
	movq	56(%rsp), Y
	movq	64(%rsp), INCY
#endif

	SAVEREGISTERS

	leaq	(, INCX, SIZE), INCX
	leaq	(, INCY, SIZE), INCY

	testq	M, M
	jle	.L29

	cmpq	$SIZE, INCX
	jne	.L20
	cmpq	$SIZE, INCY
	jne	.L20

	movq	M,  %rax
	sarq	$3, %rax
	jle	.L13

	movsd	0 * SIZE(X), %xmm0
	movsd	1 * SIZE(X), %xmm1
	movsd	2 * SIZE(X), %xmm2
	movsd	3 * SIZE(X), %xmm3

	movsd	0 * SIZE(Y), %xmm4
	movsd	1 * SIZE(Y), %xmm5
	movsd	2 * SIZE(Y), %xmm6
	movsd	3 * SIZE(Y), %xmm7

	movsd	4 * SIZE(X), %xmm8
	mulsd	ALPHA, %xmm0
	movsd	5 * SIZE(X), %xmm9
	mulsd	ALPHA, %xmm1
	movsd	6 * SIZE(X), %xmm10
	mulsd	ALPHA, %xmm2
	movsd	7 * SIZE(X), %xmm11
	mulsd	ALPHA, %xmm3

	decq	%rax
	jle .L12
	ALIGN_3

.L11:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	addsd	%xmm4, %xmm0
	movsd	4 * SIZE(Y), %xmm4
	addsd	%xmm5, %xmm1
	movsd	5 * SIZE(Y), %xmm5
	addsd	%xmm6, %xmm2
	movsd	6 * SIZE(Y), %xmm6
	addsd	%xmm7, %xmm3
	movsd	7 * SIZE(Y), %xmm7

	movsd	%xmm0, 0 * SIZE(Y)
	mulsd	ALPHA, %xmm8
	movsd	 8 * SIZE(X), %xmm0

	movsd	%xmm1, 1 * SIZE(Y)
	mulsd	ALPHA, %xmm9
	movsd	 9 * SIZE(X), %xmm1

	movsd	%xmm2, 2 * SIZE(Y)
	mulsd	ALPHA, %xmm10
	movsd	10 * SIZE(X), %xmm2

	movsd	%xmm3, 3 * SIZE(Y)
	mulsd	ALPHA, %xmm11
	movsd	11 * SIZE(X), %xmm3

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	addsd	%xmm4, %xmm8
	movsd	 8 * SIZE(Y), %xmm4
	addsd	%xmm5, %xmm9
	movsd	 9 * SIZE(Y), %xmm5
	addsd	%xmm6, %xmm10
	movsd	10 * SIZE(Y), %xmm6
	addsd	%xmm7, %xmm11
	movsd	11 * SIZE(Y), %xmm7

	movsd	%xmm8,  4 * SIZE(Y)
	mulsd	ALPHA, %xmm0
	movsd	12 * SIZE(X), %xmm8

	movsd	%xmm9,  5 * SIZE(Y)
	mulsd	ALPHA, %xmm1
	movsd	13 * SIZE(X), %xmm9

	movsd	%xmm10, 6 * SIZE(Y)
	mulsd	ALPHA, %xmm2
 	movsd	14 * SIZE(X), %xmm10

	movsd	%xmm11, 7 * SIZE(Y)
	mulsd	ALPHA, %xmm3
	movsd	15 * SIZE(X), %xmm11

	addq	$8 * SIZE, Y
	addq	$8 * SIZE, X
	decq	%rax
	jg	.L11
	ALIGN_3

.L12:
	addsd	%xmm4, %xmm0
	movsd	4 * SIZE(Y), %xmm4
	addsd	%xmm5, %xmm1
	movsd	5 * SIZE(Y), %xmm5
	addsd	%xmm6, %xmm2
	movsd	6 * SIZE(Y), %xmm6
	addsd	%xmm7, %xmm3
	movsd	7 * SIZE(Y), %xmm7

	movsd	%xmm0, 0 * SIZE(Y)
	mulsd	ALPHA, %xmm8
	movsd	%xmm1, 1 * SIZE(Y)
	mulsd	ALPHA, %xmm9
	movsd	%xmm2, 2 * SIZE(Y)
	mulsd	ALPHA, %xmm10
	movsd	%xmm3, 3 * SIZE(Y)
	mulsd	ALPHA, %xmm11

	addsd	%xmm4, %xmm8
	addsd	%xmm5, %xmm9
	addsd	%xmm6, %xmm10
	addsd	%xmm7, %xmm11

	movsd	%xmm8,  4 * SIZE(Y)
	movsd	%xmm9,  5 * SIZE(Y)
	movsd	%xmm10, 6 * SIZE(Y)
	movsd	%xmm11, 7 * SIZE(Y)

	addq	$8 * SIZE, X
	addq	$8 * SIZE, Y
	ALIGN_3

.L13:
	movq	M,  %rax
	andq	$4, %rax
	jle	.L15
	ALIGN_3

	movsd	0 * SIZE(X), %xmm0
	movsd	1 * SIZE(X), %xmm1
	movsd	2 * SIZE(X), %xmm2
	movsd	3 * SIZE(X), %xmm3

	movsd	0 * SIZE(Y), %xmm4
	mulsd	ALPHA, %xmm0
	movsd	1 * SIZE(Y), %xmm5
	mulsd	ALPHA, %xmm1
	movsd	2 * SIZE(Y), %xmm6
	mulsd	ALPHA, %xmm2
	movsd	3 * SIZE(Y), %xmm7
	mulsd	ALPHA, %xmm3

	addsd	%xmm4, %xmm0
	addsd	%xmm5, %xmm1
	addsd	%xmm6, %xmm2
	addsd	%xmm7, %xmm3

	movsd	%xmm0, 0 * SIZE(Y)
	movsd	%xmm1, 1 * SIZE(Y)
	movsd	%xmm2, 2 * SIZE(Y)
	movsd	%xmm3, 3 * SIZE(Y)

	addq	$4 * SIZE, X
	addq	$4 * SIZE, Y
	ALIGN_3

.L15:
	movq	M,  %rax
	andq	$2, %rax
	jle	.L16
	ALIGN_3

	movsd	0 * SIZE(X), %xmm0
	movsd	0 * SIZE(Y), %xmm4
	movsd	1 * SIZE(X), %xmm1
	movsd	1 * SIZE(Y), %xmm5

	mulsd	ALPHA, %xmm0
	mulsd	ALPHA, %xmm1
	addsd	%xmm4, %xmm0
	addsd	%xmm5, %xmm1

	movsd	%xmm0, 0 * SIZE(Y)
	movsd	%xmm1, 1 * SIZE(Y)

	addq	$2 * SIZE, X
	addq	$2 * SIZE, Y
	ALIGN_3

.L16:
	movq	M,  %rax
	andq	$1, %rax
	jle	.L19
	ALIGN_3

	movsd	0 * SIZE(X), %xmm0
	mulsd	ALPHA, %xmm0
	addsd	0 * SIZE(Y), %xmm0

	movsd	%xmm0, 	0 * SIZE(Y)
	addq	$SIZE, Y
	ALIGN_3

.L19:
	xorq	%rax,%rax

	RESTOREREGISTERS

	ret
	ALIGN_3

.L20:
	movq	Y, YY

	movq	M,  %rax
	sarq	$3, %rax
	jle	.L23

	movsd	(X), %xmm0
	addq	INCX, X
	movsd	(X), %xmm1
	addq	INCX, X
	movsd	(X), %xmm2
	addq	INCX, X
	movsd	(X), %xmm3
	addq	INCX, X

	movsd	(Y), %xmm4
	addq	INCY, Y
	movsd	(Y), %xmm5
	addq	INCY, Y
	movsd	(Y), %xmm6
	addq	INCY, Y
	movsd	(Y), %xmm7
	addq	INCY, Y

	movsd	(X), %xmm8
	addq	INCX, X
	mulsd	ALPHA, %xmm0
	movsd	(X), %xmm9
	addq	INCX, X
	mulsd	ALPHA, %xmm1
	movsd	(X), %xmm10
	addq	INCX, X
	mulsd	ALPHA, %xmm2
	movsd	(X), %xmm11
	addq	INCX, X
	mulsd	ALPHA, %xmm3

	decq	%rax
	jle .L22
	ALIGN_3

.L21:
	addsd	%xmm4, %xmm0
	movsd	(Y), %xmm4
	addq	INCY, Y
	addsd	%xmm5, %xmm1
	movsd	(Y), %xmm5
	addq	INCY, Y

	addsd	%xmm6, %xmm2
	movsd	(Y), %xmm6
	addq	INCY, Y
	addsd	%xmm7, %xmm3
	movsd	(Y), %xmm7
	addq	INCY, Y

	movsd	%xmm0, (YY)
	addq	INCY, YY
	movsd	(X), %xmm0
	addq	INCX, X
	mulsd	ALPHA, %xmm8

	movsd	%xmm1, (YY)
	addq	INCY, YY
	movsd	(X), %xmm1
	addq	INCX, X
	mulsd	ALPHA, %xmm9

	movsd	%xmm2, (YY)
	addq	INCY, YY
	movsd	(X), %xmm2
	addq	INCX, X
	mulsd	ALPHA, %xmm10

	movsd	%xmm3, (YY)
	addq	INCY, YY
	movsd	(X), %xmm3
	addq	INCX, X
	mulsd	ALPHA, %xmm11

	addsd	%xmm4, %xmm8
	movsd	(Y), %xmm4
	addq	INCY, Y
	addsd	%xmm5, %xmm9
	movsd	(Y), %xmm5
	addq	INCY, Y

	addsd	%xmm6, %xmm10
	movsd	(Y), %xmm6
	addq	INCY, Y
	addsd	%xmm7, %xmm11
	movsd	(Y), %xmm7
	addq	INCY, Y

	movsd	%xmm8,  (YY)
	addq	INCY, YY
	movsd	(X), %xmm8
	addq	INCX, X
	mulsd	ALPHA, %xmm0

	movsd	%xmm9,  (YY)
	addq	INCY, YY
	movsd	(X), %xmm9
	addq	INCX, X
	mulsd	ALPHA, %xmm1

	movsd	%xmm10, (YY)
	addq	INCY, YY
	movsd	(X), %xmm10
	addq	INCX, X
	mulsd	ALPHA, %xmm2

	movsd	%xmm11, (YY)
	addq	INCY, YY
	movsd	(X), %xmm11
	addq	INCX, X
	mulsd	ALPHA, %xmm3

	decq	%rax
	jg	.L21
	ALIGN_3

.L22:
	addsd	%xmm4, %xmm0
	movsd	(Y), %xmm4
	addq	INCY, Y
	addsd	%xmm5, %xmm1
	movsd	(Y), %xmm5
	addq	INCY, Y
	addsd	%xmm6, %xmm2
	movsd	(Y), %xmm6
	addq	INCY, Y
	addsd	%xmm7, %xmm3
	movsd	(Y), %xmm7
	addq	INCY, Y

	movsd	%xmm0, (YY)
	addq	INCY, YY
	mulsd	ALPHA, %xmm8

	movsd	%xmm1, (YY)
	addq	INCY, YY
	mulsd	ALPHA, %xmm9

	movsd	%xmm2, (YY)
	addq	INCY, YY
	mulsd	ALPHA, %xmm10

	movsd	%xmm3, (YY)
	addq	INCY, YY
	mulsd	ALPHA, %xmm11

	addsd	%xmm4, %xmm8
	addsd	%xmm5, %xmm9
	addsd	%xmm6, %xmm10
	addsd	%xmm7, %xmm11

	movsd	%xmm8,  (YY)
	addq	INCY, YY
	movsd	%xmm9,  (YY)
	addq	INCY, YY
	movsd	%xmm10, (YY)
	addq	INCY, YY
	movsd	%xmm11, (YY)
	addq	INCY, YY
	ALIGN_3

.L23:
	movq	M,  %rax
	andq	$4, %rax
	jle	.L25
	ALIGN_3

	movsd	(X), %xmm0
	addq	INCX, X
	movsd	(Y), %xmm4
	addq	INCY, Y
	movsd	(X), %xmm1
	addq	INCX, X
	movsd	(Y), %xmm5
	addq	INCY, Y

	movsd	(X), %xmm2
	addq	INCX, X
	mulsd	ALPHA, %xmm0
	movsd	(Y), %xmm6
	addq	INCY, Y
	mulsd	ALPHA, %xmm1
	movsd	(X), %xmm3
	addq	INCX, X
	mulsd	ALPHA, %xmm2
	movsd	(Y), %xmm7
	addq	INCY, Y
	mulsd	ALPHA, %xmm3

	addsd	%xmm4, %xmm0
	addsd	%xmm5, %xmm1
	addsd	%xmm6, %xmm2
	addsd	%xmm7, %xmm3

	movsd	%xmm0, (YY)
	addq	INCY, YY
	movsd	%xmm1, (YY)
	addq	INCY, YY
	movsd	%xmm2, (YY)
	addq	INCY, YY
	movsd	%xmm3, (YY)
	addq	INCY, YY
	ALIGN_3

.L25:
	movq	M,  %rax
	andq	$2, %rax
	jle	.L26
	ALIGN_3

	movsd	(X), %xmm0
	addq	INCX, X
	movsd	(Y), %xmm4
	addq	INCY, Y
	movsd	(X), %xmm1
	addq	INCX, X
	movsd	(Y), %xmm5
	addq	INCY, Y

	mulsd	ALPHA, %xmm0
	mulsd	ALPHA, %xmm1
	addsd	%xmm4, %xmm0
	addsd	%xmm5, %xmm1

	movsd	%xmm0, (YY)
	addq	INCY, YY
	movsd	%xmm1, (YY)
	addq	INCY, YY
	ALIGN_3

.L26:
	movq	M,  %rax
	andq	$1, %rax
	jle	.L29
	ALIGN_3

	movsd	(X), %xmm0
	mulsd	ALPHA, %xmm0
	addsd	(Y), %xmm0

	movsd	%xmm0, 	(YY)
	addq	$SIZE, Y
	ALIGN_3

.L29:
	xorq	%rax, %rax

	RESTOREREGISTERS

	ret

	EPILOGUE
