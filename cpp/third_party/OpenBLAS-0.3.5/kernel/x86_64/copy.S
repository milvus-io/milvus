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

#define N	ARG1	/* rdi */
#define X	ARG2	/* rsi */
#define INCX	ARG3	/* rdx */
#define Y	ARG4	/* rcx */
#ifndef WINDOWS_ABI
#define INCY	ARG5	/* r8  */
#define FLAG	ARG6
#else
#define INCY	%r10
#define FLAG	%r11
#endif

#include "l1param.h"

	PROLOGUE
	PROFCODE

#ifdef WINDOWS_ABI
	movq	40(%rsp), INCY
#endif

	EMMS

	testq	N, N	# if m == 0 goto End
	jle	.L999

	salq	$BASE_SHIFT, INCX
	salq	$BASE_SHIFT, INCY

	cmpq	$SIZE, INCX	# if incx != 1
	jne	.L100
	cmpq	$SIZE, INCY	# if incy != 1
	jne	.L100

	movq	N, %rax	# i = m
	sarq	$3,   %rax
	jle	.L20
	ALIGN_2

.L11:
#ifdef XDOUBLE

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movq	  0(X), %mm0
	movq	  8(X), %mm1

	movq	%mm0,   0(Y)
	movq	%mm1,   8(Y)

	movq	 16(X), %mm2
	movq	 24(X), %mm3

	movq	%mm2,  16(Y)
	movq	%mm3,  24(Y)

	movq	 32(X), %mm4
	movq	 40(X), %mm5

	movq	%mm4,  32(Y)
	movq	%mm5,  40(Y)

	movq	 48(X), %mm6
	movq	 56(X), %mm7

	movq	%mm6,  48(Y)
	movq	%mm7,  56(Y)

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	movq	 64(X), %mm0
	movq	 72(X), %mm1

	movq	%mm0,  64(Y)
	movq	%mm1,  72(Y)

	movq	 80(X), %mm2
	movq	 88(X), %mm3

	movq	%mm2,  80(Y)
	movq	%mm3,  88(Y)

	movq	 96(X), %mm4
	movq	104(X), %mm5

	movq	%mm4,  96(Y)
	movq	%mm5, 104(Y)

	movq	112(X), %mm6
	movq	120(X), %mm7

	movq	%mm6, 112(Y)
	movq	%mm7, 120(Y)
#elif defined(DOUBLE)

	movq	  0(X), %mm0
	movq	  8(X), %mm1

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movq	%mm0,   0(Y)
	movq	%mm1,   8(Y)

	movq	 16(X), %mm2
	movq	 24(X), %mm3

	movq	%mm2,  16(Y)
	movq	%mm3,  24(Y)

	movq	 32(X), %mm4
	movq	 40(X), %mm5

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	movq	%mm4,  32(Y)
	movq	%mm5,  40(Y)

	movq	 48(X), %mm6
	movq	 56(X), %mm7

	movq	%mm6,  48(Y)
	movq	%mm7,  56(Y)
#else
	movq	 0 * SIZE(X), %mm0
	movq	 2 * SIZE(X), %mm2

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movq	%mm0,  0 * SIZE(Y)
	movq	%mm2,  2 * SIZE(Y)

	movq	 4 * SIZE(X), %mm4
	movq	 6 * SIZE(X), %mm6

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	movq	%mm4,  4 * SIZE(Y)
	movq	%mm6,  6 * SIZE(Y)
#endif

	addq	$8 * SIZE, X
	addq	$8 * SIZE, Y
	decq	%rax
	jg	.L11
	ALIGN_2

.L20:
	movq	N, %rax
	andq	$7,   %rax
	jle	.L99
	ALIGN_2

.L21:
#ifdef XDOUBLE
	movq	0(X), %mm0
	movq	8(X), %mm1
	movq	%mm0, 0(Y)
	movq	%mm1, 8(Y)
#else
	MOVQ	(X), %mm0
	MOVQ	%mm0, (Y)
#endif

	addq	$SIZE, X
	addq	$SIZE, Y
	decq	%rax
	jg	.L21

.L99:
	xorq	%rax,%rax
	EMMS
	ret
	ALIGN_3

.L100:
	movq	N, %rax
	sarq	$3,   %rax
	jle	.L120
	ALIGN_2

.L111:
#ifdef XDOUBLE
	movq	0(X), %mm0
	movq	8(X), %mm1
	addq	INCX, X

	movq	0(X), %mm2
	movq	8(X), %mm3
	addq	INCX, X

	movq	0(X), %mm4
	movq	8(X), %mm5
	addq	INCX, X

	movq	0(X), %mm6
	movq	8(X), %mm7
	addq	INCX, X

	movq	%mm0, 0(Y)
	movq	%mm1, 8(Y)
	addq	INCY, Y

	movq	%mm2, 0(Y)
	movq	%mm3, 8(Y)
	addq	INCY, Y

	movq	%mm4, 0(Y)
	movq	%mm5, 8(Y)
	addq	INCY, Y

	movq	%mm6, 0(Y)
	movq	%mm7, 8(Y)
	addq	INCY, Y

	movq	0(X), %mm0
	movq	8(X), %mm1
	addq	INCX, X

	movq	0(X), %mm2
	movq	8(X), %mm3
	addq	INCX, X

	movq	0(X), %mm4
	movq	8(X), %mm5
	addq	INCX, X

	movq	0(X), %mm6
	movq	8(X), %mm7
	addq	INCX, X

	movq	%mm0, 0(Y)
	movq	%mm1, 8(Y)
	addq	INCY, Y

	movq	%mm2, 0(Y)
	movq	%mm3, 8(Y)
	addq	INCY, Y

	movq	%mm4, 0(Y)
	movq	%mm5, 8(Y)
	addq	INCY, Y

	movq	%mm6, 0(Y)
	movq	%mm7, 8(Y)
	addq	INCY, Y
#else
	MOVQ	(X), %mm0
	addq	INCX, X
	MOVQ	(X), %mm1
	addq	INCX, X
	MOVQ	(X), %mm2
	addq	INCX, X
	MOVQ	(X), %mm3
	addq	INCX, X
	MOVQ	(X), %mm4
	addq	INCX, X
	MOVQ	(X), %mm5
	addq	INCX, X
	MOVQ	(X), %mm6
	addq	INCX, X
	MOVQ	(X), %mm7
	addq	INCX, X

	MOVQ	%mm0, (Y)
	addq	INCY, Y
	MOVQ	%mm1, (Y)
	addq	INCY, Y
	MOVQ	%mm2, (Y)
	addq	INCY, Y
	MOVQ	%mm3, (Y)
	addq	INCY, Y
	MOVQ	%mm4, (Y)
	addq	INCY, Y
	MOVQ	%mm5, (Y)
	addq	INCY, Y
	MOVQ	%mm6, (Y)
	addq	INCY, Y
	MOVQ	%mm7, (Y)
	addq	INCY, Y
#endif

	decq	%rax
	jg	.L111

.L120:
	movq	N, %rax
	andq	$7,   %rax
	jle	.L999
	ALIGN_2

.L121:
#ifdef XDOUBLE
	movq	0(X), %mm0
	movq	8(X), %mm1
	movq	%mm0, 0(Y)
	movq	%mm1, 8(Y)
#else
	MOVQ	(X), %mm0
	MOVQ	%mm0, (Y)
#endif
	addq	INCX, X
	addq	INCY, Y

	decq	%rax
	jg	.L121

.L999:
	xorq	%rax,%rax
	EMMS
	ret

	EPILOGUE

