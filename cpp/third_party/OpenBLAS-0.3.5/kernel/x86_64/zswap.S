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
#define N	ARG1	/* rdi */
#define X	ARG4
#define INCX	ARG5
#define Y	ARG6
#define INCY	ARG2
#else
#define N	ARG1
#define X	ARG2
#define INCX	ARG3
#define Y	ARG4
#define INCY	%rbx
#endif

#define XX	%r10
#define YY	%r11

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
	pushq	%rbx

	movq	56(%rsp), X
	movq	64(%rsp), INCX
	movq	72(%rsp), Y
	movq	80(%rsp), INCY
#endif

	EMMS

	salq	$ZBASE_SHIFT, INCX
	salq	$ZBASE_SHIFT, INCY

	cmpq	$2 * SIZE, INCX
	jne	.L14
	cmpq	$2 * SIZE, INCY
	jne	.L14

	movq	N,  %rax
	sarq	$2, %rax
	jle	.L15
	ALIGN_3

.L16:
#ifdef XDOUBLE
	movq	  0(X), %mm0
	movq	  8(X), %mm1
	movq	 16(X), %mm2
	movq	 24(X), %mm3
	movq	  0(Y), %mm4
	movq	  8(Y), %mm5
	movq	 16(Y), %mm6
	movq	 24(Y), %mm7

	movq	%mm4,   0(X)
	movq	%mm5,   8(X)
	movq	%mm6,  16(X)
	movq	%mm7,  24(X)
	movq	%mm0,   0(Y)
	movq	%mm1,   8(Y)
	movq	%mm2,  16(Y)
	movq	%mm3,  24(Y)

	movq	 32(X), %mm0
	movq	 40(X), %mm1
	movq	 48(X), %mm2
	movq	 56(X), %mm3
	movq	 32(Y), %mm4
	movq	 40(Y), %mm5
	movq	 48(Y), %mm6
	movq	 56(Y), %mm7

	movq	%mm4,  32(X)
	movq	%mm5,  40(X)
	movq	%mm6,  48(X)
	movq	%mm7,  56(X)
	movq	%mm0,  32(Y)
	movq	%mm1,  40(Y)
	movq	%mm2,  48(Y)
	movq	%mm3,  56(Y)

	movq	 64(X), %mm0
	movq	 72(X), %mm1
	movq	 80(X), %mm2
	movq	 88(X), %mm3
	movq	 64(Y), %mm4
	movq	 72(Y), %mm5
	movq	 80(Y), %mm6
	movq	 88(Y), %mm7

	movq	%mm4,  64(X)
	movq	%mm5,  72(X)
	movq	%mm6,  80(X)
	movq	%mm7,  88(X)
	movq	%mm0,  64(Y)
	movq	%mm1,  72(Y)
	movq	%mm2,  80(Y)
	movq	%mm3,  88(Y)

	movq	 96(X), %mm0
	movq	104(X), %mm1
	movq	112(X), %mm2
	movq	120(X), %mm3
	movq	 96(Y), %mm4
	movq	104(Y), %mm5
	movq	112(Y), %mm6
	movq	120(Y), %mm7

	movq	%mm4,  96(X)
	movq	%mm5, 104(X)
	movq	%mm6, 112(X)
	movq	%mm7, 120(X)
	movq	%mm0,  96(Y)
	movq	%mm1, 104(Y)
	movq	%mm2, 112(Y)
	movq	%mm3, 120(Y)
#elif defined(DOUBLE)
	prefetchw	PREFETCHSIZE * SIZE(X)
	MOVQ	0 * SIZE(X), %mm0
	MOVQ	1 * SIZE(X), %mm1
	MOVQ	2 * SIZE(X), %mm2
	MOVQ	3 * SIZE(X), %mm3
	prefetchw	PREFETCHSIZE * SIZE(Y)
	MOVQ	0 * SIZE(Y), %mm4
	MOVQ	1 * SIZE(Y), %mm5
	MOVQ	2 * SIZE(Y), %mm6
	MOVQ	3 * SIZE(Y), %mm7

	MOVQ	%mm4, 0 * SIZE(X)
	MOVQ	%mm5, 1 * SIZE(X)
	MOVQ	%mm6, 2 * SIZE(X)
	MOVQ	%mm7, 3 * SIZE(X)
	MOVQ	%mm0, 0 * SIZE(Y)
	MOVQ	%mm1, 1 * SIZE(Y)
	MOVQ	%mm2, 2 * SIZE(Y)
	MOVQ	%mm3, 3 * SIZE(Y)

	MOVQ	4 * SIZE(X), %mm0
	MOVQ	5 * SIZE(X), %mm1
	MOVQ	6 * SIZE(X), %mm2
	MOVQ	7 * SIZE(X), %mm3
	MOVQ	4 * SIZE(Y), %mm4
	MOVQ	5 * SIZE(Y), %mm5
	MOVQ	6 * SIZE(Y), %mm6
	MOVQ	7 * SIZE(Y), %mm7

	MOVQ	%mm4, 4 * SIZE(X)
	MOVQ	%mm5, 5 * SIZE(X)
	MOVQ	%mm6, 6 * SIZE(X)
	MOVQ	%mm7, 7 * SIZE(X)
	MOVQ	%mm0, 4 * SIZE(Y)
	MOVQ	%mm1, 5 * SIZE(Y)
	MOVQ	%mm2, 6 * SIZE(Y)
	MOVQ	%mm3, 7 * SIZE(Y)

#else
#ifdef OPTERON
	prefetchw	PREFETCHSIZE * SIZE(X)
#endif
	movq	0 * SIZE(X), %mm0
	movq	2 * SIZE(X), %mm1
	movq	4 * SIZE(X), %mm2
	movq	6 * SIZE(X), %mm3
	movq	0 * SIZE(Y), %mm4
	movq	2 * SIZE(Y), %mm5
	movq	4 * SIZE(Y), %mm6
	movq	6 * SIZE(Y), %mm7

#ifdef OPTERON
	prefetchw	PREFETCHSIZE * SIZE(Y)
#endif
	movq	%mm4, 0 * SIZE(X)
	movq	%mm5, 2 * SIZE(X)
	movq	%mm6, 4 * SIZE(X)
	movq	%mm7, 6 * SIZE(X)

	movq	%mm0, 0 * SIZE(Y)
	movq	%mm1, 2 * SIZE(Y)
	movq	%mm2, 4 * SIZE(Y)
	movq	%mm3, 6 * SIZE(Y)
#endif

	addq	$8 * SIZE, X
	addq	$8 * SIZE, Y
	decq	%rax
	jg	.L16
	ALIGN_3

.L15:
	movq	N, %rax
	andq	$3, %rax
	jle	.L27
	ALIGN_3

.L22:
#ifdef XDOUBLE
	movq	  0(X), %mm0
	movq	  8(X), %mm1
	movq	 16(X), %mm2
	movq	 24(X), %mm3
	movq	  0(Y), %mm4
	movq	  8(Y), %mm5
	movq	 16(Y), %mm6
	movq	 24(Y), %mm7

	movq	%mm4,   0(X)
	movq	%mm5,   8(X)
	movq	%mm6,  16(X)
	movq	%mm7,  24(X)
	movq	%mm0,   0(Y)
	movq	%mm1,   8(Y)
	movq	%mm2,  16(Y)
	movq	%mm3,  24(Y)
#elif defined(DOUBLE)
	movq	0 * SIZE(X), %mm0
	movq	1 * SIZE(X), %mm1
	movq	0 * SIZE(Y), %mm4
	movq	1 * SIZE(Y), %mm5
	movq	%mm4, 0 * SIZE(X)
	movq	%mm5, 1 * SIZE(X)
	movq	%mm0, 0 * SIZE(Y)
	movq	%mm1, 1 * SIZE(Y)
#else
	movq	0 * SIZE(X), %mm0
	movq	0 * SIZE(Y), %mm4
	movq	%mm4, 0 * SIZE(X)
	movq	%mm0, 0 * SIZE(Y)
#endif
	addq	$2 * SIZE, X
	addq	$2 * SIZE, Y
	decq	%rax
	jg	.L22
	jmp	.L27
	ALIGN_3

/* INCX != 1 or INCY != 1 */

.L14:
	movq	N, %rax
	movq	X, XX
	movq	Y, YY
	sarq	$1,   %rax
	jle	.L28
	ALIGN_2

.L29:
#ifdef XDOUBLE
	movq	  0(X), %mm0
	movq	  8(X), %mm1
	movq	 16(X), %mm2
	movq	 24(X), %mm3
	addq	INCX, X
	movq	  0(Y), %mm4
	movq	  8(Y), %mm5
	movq	 16(Y), %mm6
	movq	 24(Y), %mm7
	addq	INCY, Y

	movq	%mm4,   0(XX)
	movq	%mm5,   8(XX)
	movq	%mm6,  16(XX)
	movq	%mm7,  24(XX)
	addq	INCX, XX
	movq	%mm0,   0(YY)
	movq	%mm1,   8(YY)
	movq	%mm2,  16(YY)
	movq	%mm3,  24(YY)
	addq	INCY, YY

	movq	  0(X), %mm0
	movq	  8(X), %mm1
	movq	 16(X), %mm2
	movq	 24(X), %mm3
	addq	INCX, X
	movq	  0(Y), %mm4
	movq	  8(Y), %mm5
	movq	 16(Y), %mm6
	movq	 24(Y), %mm7
	addq	INCY, Y

	movq	%mm4,   0(XX)
	movq	%mm5,   8(XX)
	movq	%mm6,  16(XX)
	movq	%mm7,  24(XX)
	addq	INCX, XX
	movq	%mm0,   0(YY)
	movq	%mm1,   8(YY)
	movq	%mm2,  16(YY)
	movq	%mm3,  24(YY)
	addq	INCY, YY
#elif defined(DOUBLE)
	movq	0 * SIZE(X), %mm0
	movq	1 * SIZE(X), %mm1
	addq	INCX, X
	movq	0 * SIZE(X), %mm2
	movq	1 * SIZE(X), %mm3
	addq	INCX, X

	movq	0 * SIZE(Y), %mm4
	movq	1 * SIZE(Y), %mm5
	addq	INCY, Y
	movq	0 * SIZE(Y), %mm6
	movq	1 * SIZE(Y), %mm7
	addq	INCY, Y

	movq	%mm4, 0 * SIZE(XX)
	movq	%mm5, 1 * SIZE(XX)
	addq	INCX, XX
	movq	%mm6, 0 * SIZE(XX)
	movq	%mm7, 1 * SIZE(XX)
	addq	INCX, XX

	movq	%mm0, 0 * SIZE(YY)
	movq	%mm1, 1 * SIZE(YY)
	addq	INCY, YY
	movq	%mm2, 0 * SIZE(YY)
	movq	%mm3, 1 * SIZE(YY)
	addq	INCY, YY
#else
	movq	0 * SIZE(X), %mm0
	addq	INCX, X
	movq	0 * SIZE(X), %mm2
	addq	INCX, X

	movq	0 * SIZE(Y), %mm4
	addq	INCY, Y
	movq	0 * SIZE(Y), %mm6
	addq	INCY, Y

	movq	%mm4, 0 * SIZE(XX)
	addq	INCX, XX
	movq	%mm6, 0 * SIZE(XX)
	addq	INCX, XX

	movq	%mm0, 0 * SIZE(YY)
	addq	INCY, YY
	movq	%mm2, 0 * SIZE(YY)
	addq	INCY, YY
#endif
	decq	%rax
	jg	.L29
	ALIGN_3

.L28:
	movq	N, %rax
	andq	$1,   %rax
	jle	.L27
	ALIGN_3

.L35:
#ifdef XDOUBLE
	movq	  0(X), %mm0
	movq	  8(X), %mm1
	movq	 16(X), %mm2
	movq	 24(X), %mm3
	movq	  0(Y), %mm4
	movq	  8(Y), %mm5
	movq	 16(Y), %mm6
	movq	 24(Y), %mm7

	movq	%mm4,   0(X)
	movq	%mm5,   8(X)
	movq	%mm6,  16(X)
	movq	%mm7,  24(X)
	movq	%mm0,   0(Y)
	movq	%mm1,   8(Y)
	movq	%mm2,  16(Y)
	movq	%mm3,  24(Y)
#elif defined(DOUBLE)
	movq	0 * SIZE(X), %mm0
	movq	1 * SIZE(X), %mm1
	movq	0 * SIZE(Y), %mm4
	movq	1 * SIZE(Y), %mm5

	movq	%mm4, 0 * SIZE(X)
	movq	%mm5, 1 * SIZE(X)
	movq	%mm0, 0 * SIZE(Y)
	movq	%mm1, 1 * SIZE(Y)
#else
	movq	0 * SIZE(X), %mm0
	movq	0 * SIZE(Y), %mm4
	movq	%mm4, 0 * SIZE(X)
	movq	%mm0, 0 * SIZE(Y)
#endif
	addq	INCX, X
	addq	INCY, Y

	decq	%rax
	jg	.L35
	ALIGN_3

.L27:
	EMMS
	xorq	%rax,%rax

#ifdef WINDOWS_ABI
	popq	%rbx
#endif

	ret

	EPILOGUE
