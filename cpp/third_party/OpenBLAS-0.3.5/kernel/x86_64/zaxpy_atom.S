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
#ifndef XDOUBLE
	movq	 8(%rsp), INCY
#else
	movq	40(%rsp), INCY
#endif
#else
	movaps	%xmm3, %xmm0
	movsd	40(%rsp), %xmm1

	movq	48(%rsp), X
	movq	56(%rsp), INCX
	movq	64(%rsp), Y
	movq	72(%rsp), INCY
#endif

	SAVEREGISTERS

#ifndef CONJ
#define ADD1	subsd
#define ADD2	addsd
#else
#define ADD1	addsd
#define ADD2	subsd
#endif

	salq	$ZBASE_SHIFT, INCX
	movaps	%xmm0, ALPHA_R
	salq	$ZBASE_SHIFT, INCY
	movaps	%xmm1, ALPHA_I

	testq	M, M
	jle	.L999

	cmpq	$2 * SIZE, INCX
	jne	.L20
	cmpq	$2 * SIZE, INCY
	jne	.L20

	movq	M,  %rax
	sarq	$2, %rax
	jle	.L15

	movsd	 0 * SIZE(X), %xmm0
	movsd	 1 * SIZE(X), %xmm1
	movsd	 0 * SIZE(Y), %xmm8
	movsd	 1 * SIZE(Y), %xmm9

	movsd	 2 * SIZE(X), %xmm4
	movsd	 3 * SIZE(X), %xmm5
	movsd	 2 * SIZE(Y), %xmm10
	movsd	 3 * SIZE(Y), %xmm11

	movaps	 %xmm0,  %xmm2
	mulsd	ALPHA_R, %xmm0
	movaps	 %xmm1,  %xmm3
	mulsd	ALPHA_R, %xmm1
	mulsd	ALPHA_I, %xmm3
	mulsd	ALPHA_I, %xmm2

	movaps	 %xmm4,  %xmm6
	mulsd	ALPHA_R, %xmm4
	addsd	%xmm0, %xmm8
	movsd	 4 * SIZE(X), %xmm0

	movaps	 %xmm5,  %xmm7
	mulsd	ALPHA_R, %xmm5
	ADD2	%xmm1, %xmm9
	movsd	 5 * SIZE(X), %xmm1

	decq	%rax
	jle	.L12
	ALIGN_3

.L11:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	mulsd	ALPHA_I, %xmm7
	movsd	 4 * SIZE(Y), %xmm12
	ADD1	%xmm3, %xmm8

	mulsd	ALPHA_I, %xmm6
	movsd	 5 * SIZE(Y), %xmm13
	addsd	%xmm2, %xmm9

	addsd	%xmm4, %xmm10
	movsd	 6 * SIZE(X), %xmm4
	movaps	 %xmm0,  %xmm2
	mulsd	ALPHA_R, %xmm0

	ADD2	%xmm5, %xmm11
	movsd	 7 * SIZE(X), %xmm5
	movaps	 %xmm1,  %xmm3
	mulsd	ALPHA_R, %xmm1

	ADD1	%xmm7, %xmm10
	movsd	%xmm8,   0 * SIZE(Y)
	mulsd	ALPHA_I, %xmm3

	addsd	%xmm6, %xmm11
	movsd	%xmm9,   1 * SIZE(Y)
	mulsd	ALPHA_I, %xmm2

	movaps	 %xmm4,  %xmm6
	movsd	%xmm10,  2 * SIZE(Y)
	mulsd	ALPHA_R, %xmm4
	movsd	 6 * SIZE(Y), %xmm10
	addsd	%xmm0, %xmm12
	movsd	 8 * SIZE(X), %xmm0

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	movaps	 %xmm5,  %xmm7
	movsd	%xmm11,  3 * SIZE(Y)
	mulsd	ALPHA_R, %xmm5
	movsd	 7 * SIZE(Y), %xmm11
	ADD2	%xmm1, %xmm13
	movsd	 9 * SIZE(X), %xmm1

	mulsd	ALPHA_I, %xmm7
	movsd	 8 * SIZE(Y), %xmm8
	ADD1	%xmm3, %xmm12

	mulsd	ALPHA_I, %xmm6
	movsd	 9 * SIZE(Y), %xmm9
	addsd	%xmm2, %xmm13

	movaps	 %xmm0,  %xmm2
	mulsd	ALPHA_R, %xmm0
	addsd	%xmm4, %xmm10
	movsd	10 * SIZE(X), %xmm4

	movaps	 %xmm1,  %xmm3
	mulsd	ALPHA_R, %xmm1
	ADD2	%xmm5, %xmm11
	movsd	11 * SIZE(X), %xmm5

	mulsd	ALPHA_I, %xmm3
	movsd	%xmm12,   4 * SIZE(Y)
	ADD1	%xmm7, %xmm10

	mulsd	ALPHA_I, %xmm2
	movsd	%xmm13,   5 * SIZE(Y)
	addsd	%xmm6, %xmm11

	movaps	 %xmm4,  %xmm6
	movsd	%xmm10,  6 * SIZE(Y)
	mulsd	ALPHA_R, %xmm4
	addsd	%xmm0, %xmm8
	movsd	10 * SIZE(Y), %xmm10
	movsd	12 * SIZE(X), %xmm0

	movaps	 %xmm5,  %xmm7
	movsd	%xmm11,  7 * SIZE(Y)
	mulsd	ALPHA_R, %xmm5
	movsd	11 * SIZE(Y), %xmm11
	ADD2	%xmm1, %xmm9
	movsd	13 * SIZE(X), %xmm1

	addq	$8 * SIZE, X
	addq	$8 * SIZE, Y
	decq	%rax
	jg	.L11
	ALIGN_3

.L12:
	mulsd	ALPHA_I, %xmm7
	movsd	 4 * SIZE(Y), %xmm12
	ADD1	%xmm3, %xmm8

	mulsd	ALPHA_I, %xmm6
	movsd	 5 * SIZE(Y), %xmm13
	addsd	%xmm2, %xmm9

	addsd	%xmm4, %xmm10
	movsd	 6 * SIZE(X), %xmm4
	movaps	 %xmm0,  %xmm2
	mulsd	ALPHA_R, %xmm0

	ADD2	%xmm5, %xmm11
	movsd	 7 * SIZE(X), %xmm5
	movaps	 %xmm1,  %xmm3
	mulsd	ALPHA_R, %xmm1

	ADD1	%xmm7, %xmm10
	movsd	%xmm8,   0 * SIZE(Y)
	mulsd	ALPHA_I, %xmm3

	addsd	%xmm6, %xmm11
	movsd	%xmm9,   1 * SIZE(Y)
	mulsd	ALPHA_I, %xmm2

	movaps	 %xmm4,  %xmm6
	movsd	%xmm10,  2 * SIZE(Y)
	mulsd	ALPHA_R, %xmm4
	movsd	 6 * SIZE(Y), %xmm10
	addsd	%xmm0, %xmm12

	movaps	 %xmm5,  %xmm7
	movsd	%xmm11,  3 * SIZE(Y)
	mulsd	ALPHA_R, %xmm5
	ADD2	%xmm1, %xmm13
	movsd	 7 * SIZE(Y), %xmm11

	mulsd	ALPHA_I, %xmm7
	ADD1	%xmm3, %xmm12

	mulsd	ALPHA_I, %xmm6
	addsd	%xmm2, %xmm13

	movaps	 %xmm0,  %xmm2
	mulsd	ALPHA_R, %xmm0
	addsd	%xmm4, %xmm10

	movaps	 %xmm1,  %xmm3
	mulsd	ALPHA_R, %xmm1
	ADD2	%xmm5, %xmm11

	mulsd	ALPHA_I, %xmm3
	ADD1	%xmm7, %xmm10

	addsd	%xmm6, %xmm11
	mulsd	ALPHA_I, %xmm2

	movsd	%xmm12,  4 * SIZE(Y)
	movsd	%xmm13,  5 * SIZE(Y)
	movsd	%xmm10,  6 * SIZE(Y)
	movsd	%xmm11,  7 * SIZE(Y)

	addq	$8 * SIZE, X
	addq	$8 * SIZE, Y
	ALIGN_3

.L15:
	movq	M,  %rax
	andq	$2, %rax
	jle	.L17

	movsd	 0 * SIZE(X), %xmm0
	movsd	 1 * SIZE(X), %xmm1
	movsd	 2 * SIZE(X), %xmm4
	movsd	 3 * SIZE(X), %xmm5

	movaps	 %xmm0,  %xmm2
	movsd	 0 * SIZE(Y), %xmm8
	mulsd	ALPHA_R, %xmm0
	movaps	 %xmm1,  %xmm3
	movsd	 1 * SIZE(Y), %xmm9
	mulsd	ALPHA_R, %xmm1
	movsd	 2 * SIZE(Y), %xmm10
	mulsd	ALPHA_I, %xmm3
	movsd	 3 * SIZE(Y), %xmm11
	mulsd	ALPHA_I, %xmm2

	movaps	 %xmm4,  %xmm6
	mulsd	ALPHA_R, %xmm4
	addsd	%xmm0, %xmm8

	movaps	 %xmm5,  %xmm7
	mulsd	ALPHA_R, %xmm5
	ADD2	%xmm1, %xmm9

	mulsd	ALPHA_I, %xmm7
	ADD1	%xmm3, %xmm8

	mulsd	ALPHA_I, %xmm6
	addsd	%xmm2, %xmm9

	addsd	%xmm4, %xmm10
	movsd	%xmm8,   0 * SIZE(Y)
	ADD2	%xmm5, %xmm11
	movsd	%xmm9,   1 * SIZE(Y)
	ADD1	%xmm7, %xmm10
	addsd	%xmm6, %xmm11

	movsd	%xmm10,   2 * SIZE(Y)
	movsd	%xmm11,   3 * SIZE(Y)

	addq	$4 * SIZE, X
	addq	$4 * SIZE, Y
	ALIGN_3

.L17:
	movq	M,  %rax
	andq	$1, %rax
	jle	.L999

	movsd	 0 * SIZE(X), %xmm0
	movsd	 1 * SIZE(X), %xmm1
	movsd	 0 * SIZE(Y), %xmm8
	movsd	 1 * SIZE(Y), %xmm9

	movaps	 %xmm0,  %xmm2
	mulsd	ALPHA_R, %xmm0
	movaps	 %xmm1,  %xmm3
	mulsd	ALPHA_R, %xmm1
	mulsd	ALPHA_I, %xmm3
	mulsd	ALPHA_I, %xmm2

	addsd	%xmm0, %xmm8
	ADD2	%xmm1, %xmm9
	ADD1	%xmm3, %xmm8
	addsd	%xmm2, %xmm9

	movsd	%xmm8,   0 * SIZE(Y)
	movsd	%xmm9,   1 * SIZE(Y)
	jmp	.L999
	ALIGN_3

.L20:
	movq	Y, YY

	movq	M,  %rax
	sarq	$2, %rax
	jle	.L25

	movsd	 0 * SIZE(X), %xmm0
	movsd	 1 * SIZE(X), %xmm1
	addq	 INCX, X
	movsd	 0 * SIZE(Y), %xmm8
	movsd	 1 * SIZE(Y), %xmm9
	addq	 INCY, Y

	movsd	 0 * SIZE(X), %xmm4
	movsd	 1 * SIZE(X), %xmm5
	addq	 INCX, X
	movsd	 0 * SIZE(Y), %xmm10
	movsd	 1 * SIZE(Y), %xmm11
	addq	 INCY, Y

	movaps	 %xmm0,  %xmm2
	mulsd	ALPHA_R, %xmm0
	movaps	 %xmm1,  %xmm3
	mulsd	ALPHA_R, %xmm1
	mulsd	ALPHA_I, %xmm3
	mulsd	ALPHA_I, %xmm2

	movaps	 %xmm4,  %xmm6
	mulsd	ALPHA_R, %xmm4
	addsd	%xmm0, %xmm8
	movsd	 0 * SIZE(X), %xmm0

	movaps	 %xmm5,  %xmm7
	mulsd	ALPHA_R, %xmm5
	ADD2	%xmm1, %xmm9
	movsd	 1 * SIZE(X), %xmm1
	addq	 INCX, X

	decq	%rax
	jle	.L22
	ALIGN_3

.L21:
	mulsd	ALPHA_I, %xmm7
	movsd	 0 * SIZE(Y), %xmm12
	ADD1	%xmm3, %xmm8

	mulsd	ALPHA_I, %xmm6
	movsd	 1 * SIZE(Y), %xmm13
	addsd	%xmm2, %xmm9
	addq	 INCY, Y

	addsd	%xmm4, %xmm10
	movsd	 0 * SIZE(X), %xmm4
	movaps	 %xmm0,  %xmm2
	mulsd	ALPHA_R, %xmm0

	ADD2	%xmm5, %xmm11
	movsd	 1 * SIZE(X), %xmm5
	movaps	 %xmm1,  %xmm3
	addq	 INCX, X
	mulsd	ALPHA_R, %xmm1

	ADD1	%xmm7, %xmm10
	movsd	%xmm8,   0 * SIZE(YY)
	mulsd	ALPHA_I, %xmm3

	addsd	%xmm6, %xmm11
	movsd	%xmm9,   1 * SIZE(YY)
	mulsd	ALPHA_I, %xmm2
	addq	 INCY, YY

	movaps	 %xmm4,  %xmm6
	movsd	%xmm10,  0 * SIZE(YY)
	mulsd	ALPHA_R, %xmm4
	movsd	 0 * SIZE(Y), %xmm10
	addsd	%xmm0, %xmm12
	movsd	 0 * SIZE(X), %xmm0

	movaps	 %xmm5,  %xmm7
	movsd	%xmm11,  1 * SIZE(YY)
	addq	 INCY, YY
	mulsd	ALPHA_R, %xmm5
	movsd	 1 * SIZE(Y), %xmm11
	addq	 INCY, Y
	ADD2	%xmm1, %xmm13
	movsd	 1 * SIZE(X), %xmm1
	addq	 INCX, X

	mulsd	ALPHA_I, %xmm7
	movsd	 0 * SIZE(Y), %xmm8
	ADD1	%xmm3, %xmm12

	mulsd	ALPHA_I, %xmm6
	movsd	 1 * SIZE(Y), %xmm9
	addsd	%xmm2, %xmm13
	addq	 INCY, Y

	movaps	 %xmm0,  %xmm2
	mulsd	ALPHA_R, %xmm0
	addsd	%xmm4, %xmm10
	movsd	 0 * SIZE(X), %xmm4

	movaps	 %xmm1,  %xmm3
	mulsd	ALPHA_R, %xmm1
	ADD2	%xmm5, %xmm11
	movsd	 1 * SIZE(X), %xmm5
	addq	 INCX, X

	mulsd	ALPHA_I, %xmm3
	movsd	%xmm12,   0 * SIZE(YY)
	ADD1	%xmm7, %xmm10

	mulsd	ALPHA_I, %xmm2
	movsd	%xmm13,   1 * SIZE(YY)
	addsd	%xmm6, %xmm11
	addq	 INCY, YY

	movaps	 %xmm4,  %xmm6
	movsd	%xmm10,  0 * SIZE(YY)
	mulsd	ALPHA_R, %xmm4
	addsd	%xmm0, %xmm8
	movsd	 0 * SIZE(Y), %xmm10
	movsd	 0 * SIZE(X), %xmm0

	movaps	 %xmm5,  %xmm7
	movsd	%xmm11,  1 * SIZE(YY)
	addq	 INCY, YY
	mulsd	ALPHA_R, %xmm5
	movsd	 1 * SIZE(Y), %xmm11
	addq	 INCY, Y
	ADD2	%xmm1, %xmm9
	movsd	 1 * SIZE(X), %xmm1
	addq	 INCX, X

	decq	%rax
	jg	.L21
	ALIGN_3

.L22:
	mulsd	ALPHA_I, %xmm7
	movsd	 0 * SIZE(Y), %xmm12
	ADD1	%xmm3, %xmm8

	mulsd	ALPHA_I, %xmm6
	movsd	 1 * SIZE(Y), %xmm13
	addsd	%xmm2, %xmm9
	addq	 INCY, Y

	addsd	%xmm4, %xmm10
	movsd	 0 * SIZE(X), %xmm4
	movaps	 %xmm0,  %xmm2
	mulsd	ALPHA_R, %xmm0

	ADD2	%xmm5, %xmm11
	movsd	 1 * SIZE(X), %xmm5
	movaps	 %xmm1,  %xmm3
	addq	 INCX, X
	mulsd	ALPHA_R, %xmm1

	ADD1	%xmm7, %xmm10
	movsd	%xmm8,   0 * SIZE(YY)
	mulsd	ALPHA_I, %xmm3

	addsd	%xmm6, %xmm11
	movsd	%xmm9,   1 * SIZE(YY)
	mulsd	ALPHA_I, %xmm2
	addq	 INCY, YY

	movaps	 %xmm4,  %xmm6
	movsd	%xmm10,  0 * SIZE(YY)
	mulsd	ALPHA_R, %xmm4
	movsd	 0 * SIZE(Y), %xmm10
	addsd	%xmm0, %xmm12

	movaps	 %xmm5,  %xmm7
	movsd	%xmm11,  1 * SIZE(YY)
	mulsd	ALPHA_R, %xmm5
	addq	 INCY, YY
	ADD2	%xmm1, %xmm13
	movsd	 1 * SIZE(Y), %xmm11

	mulsd	ALPHA_I, %xmm7
	addq	 INCY, Y
	ADD1	%xmm3, %xmm12

	mulsd	ALPHA_I, %xmm6
	addsd	%xmm2, %xmm13

	movaps	 %xmm0,  %xmm2
	mulsd	ALPHA_R, %xmm0
	addsd	%xmm4, %xmm10

	movaps	 %xmm1,  %xmm3
	mulsd	ALPHA_R, %xmm1
	ADD2	%xmm5, %xmm11

	mulsd	ALPHA_I, %xmm3
	ADD1	%xmm7, %xmm10

	addsd	%xmm6, %xmm11
	mulsd	ALPHA_I, %xmm2

	movsd	%xmm12,  0 * SIZE(YY)
	movsd	%xmm13,  1 * SIZE(YY)
	addq	 INCY, YY
	movsd	%xmm10,  0 * SIZE(YY)
	movsd	%xmm11,  1 * SIZE(YY)
	addq	 INCY, YY
	ALIGN_3

.L25:
	movq	M,  %rax
	andq	$2, %rax
	jle	.L27

	movsd	 0 * SIZE(X), %xmm0
	movsd	 1 * SIZE(X), %xmm1
	addq	 INCX, X
	movsd	 0 * SIZE(X), %xmm4
	movsd	 1 * SIZE(X), %xmm5
	addq	 INCX, X

	movaps	 %xmm0,  %xmm2
	movsd	 0 * SIZE(Y), %xmm8
	mulsd	ALPHA_R, %xmm0
	movaps	 %xmm1,  %xmm3
	movsd	 1 * SIZE(Y), %xmm9
	addq	 INCY, Y
	mulsd	ALPHA_R, %xmm1
	movsd	 0 * SIZE(Y), %xmm10
	mulsd	ALPHA_I, %xmm3
	movsd	 1 * SIZE(Y), %xmm11
	mulsd	ALPHA_I, %xmm2
	addq	 INCY, Y

	movaps	 %xmm4,  %xmm6
	mulsd	ALPHA_R, %xmm4
	addsd	%xmm0, %xmm8

	movaps	 %xmm5,  %xmm7
	mulsd	ALPHA_R, %xmm5
	ADD2	%xmm1, %xmm9

	mulsd	ALPHA_I, %xmm7
	ADD1	%xmm3, %xmm8

	mulsd	ALPHA_I, %xmm6
	addsd	%xmm2, %xmm9

	addsd	%xmm4, %xmm10
	movsd	%xmm8,   0 * SIZE(YY)
	ADD2	%xmm5, %xmm11
	movsd	%xmm9,   1 * SIZE(YY)
	ADD1	%xmm7, %xmm10
	addq	 INCY, YY
	addsd	%xmm6, %xmm11

	movsd	%xmm10,  0 * SIZE(YY)
	movsd	%xmm11,  1 * SIZE(YY)
	addq	 INCY, YY
	ALIGN_3

.L27:
	movq	M,  %rax
	andq	$1, %rax
	jle	.L999

	movsd	 0 * SIZE(X), %xmm0
	movsd	 1 * SIZE(X), %xmm1
	movsd	 0 * SIZE(Y), %xmm8
	movsd	 1 * SIZE(Y), %xmm9

	movaps	 %xmm0,  %xmm2
	mulsd	ALPHA_R, %xmm0
	movaps	 %xmm1,  %xmm3
	mulsd	ALPHA_R, %xmm1
	mulsd	ALPHA_I, %xmm3
	mulsd	ALPHA_I, %xmm2

	addsd	%xmm0, %xmm8
	ADD2	%xmm1, %xmm9
	ADD1	%xmm3, %xmm8
	addsd	%xmm2, %xmm9

	movsd	%xmm8,   0 * SIZE(YY)
	movsd	%xmm9,   1 * SIZE(YY)
	ALIGN_3

.L999:
	xorq	%rax, %rax

	RESTOREREGISTERS

	ret

	EPILOGUE
