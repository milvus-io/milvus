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

#if defined(DOUBLE) || defined(XDOUBLE)
#define RESULT		 4 + STACK + ARGS(%esp)
#define STACK_N		 8 + STACK + ARGS(%esp)
#define STACK_X		12 + STACK + ARGS(%esp)
#define STACK_INCX	16 + STACK + ARGS(%esp)
#define STACK_Y		20 + STACK + ARGS(%esp)
#define STACK_INCY	24 + STACK + ARGS(%esp)
#else
#define STACK_N		 4 + STACK + ARGS(%esp)
#define STACK_X		 8 + STACK + ARGS(%esp)
#define STACK_INCX	12 + STACK + ARGS(%esp)
#define STACK_Y		16 + STACK + ARGS(%esp)
#define STACK_INCY	20 + STACK + ARGS(%esp)
#endif

#define N	%ebx
#define X	%esi
#define INCX	%ecx
#define Y	%edi
#define INCY	%edx

#include "l1param.h"

	PROLOGUE
	PROFCODE

	pushl	%edi
	pushl	%esi
	pushl	%ebx

#if defined(F_INTERFACE_GFORT) || defined(F_INTERFACE_G95)
	EMMS
#endif

	movl	STACK_N,     N
	movl	STACK_X,     X
	movl	STACK_INCX,  INCX
	movl	STACK_Y,     Y
	movl	STACK_INCY,  INCY

#ifdef F_INTERFACE
	movl	(N),N
	movl	(INCX),INCX
	movl	(INCY),INCY
#endif

#if defined(F_INTERFACE_GFORT) || defined(F_INTERFACE_G95)
	EMMS
#endif

	testl	N, N
	jle	.L88

	addl	INCX, INCX
	fldz
	addl	INCY, INCY
	fldz

	leal	(, INCX, SIZE), INCX
	fldz
	leal	(, INCY, SIZE), INCY
	fldz

	cmpl	$2 * SIZE, INCX
	jne	.L14
	cmpl	$2 * SIZE, INCY
	jne	.L14

	movl	N,  %eax
	sarl	$1, %eax
	jle	.L15
	ALIGN_3

.L16:
	FLD	 0 * SIZE(X)

	FLD	 0 * SIZE(Y)
	fmul	%st(1), %st
	faddp	%st, %st(2)

	FMUL	 1 * SIZE(Y)
	faddp	%st, %st(2)
	FLD	 1 * SIZE(X)

	FLD	 0 * SIZE(Y)
	fmul	%st(1), %st
	faddp	%st, %st(4)

	FMUL	 1 * SIZE(Y)
	faddp	%st, %st(4)
	FLD	 2 * SIZE(X)

	FLD	 2 * SIZE(Y)
	fmul	%st(1), %st
	faddp	%st, %st(2)

	FMUL	 3 * SIZE(Y)
	faddp	%st, %st(2)
	FLD	 3 * SIZE(X)

	FLD	 2 * SIZE(Y)
	fmul	%st(1), %st
	faddp	%st, %st(4)

	FMUL	 3 * SIZE(Y)
	faddp	%st, %st(4)

	addl	$4 * SIZE, X
	addl	$4 * SIZE, Y
	decl	%eax
	jg	.L16
	ALIGN_3

.L15:
	movl	N, %eax
	andl	$1,   %eax
	jle	.L27
	ALIGN_3

.L22:
	FLD	 0 * SIZE(X)

	FLD	 0 * SIZE(Y)
	fmul	%st(1), %st
	faddp	%st, %st(2)

	FMUL	 1 * SIZE(Y)
	faddp	%st, %st(2)
	FLD	 1 * SIZE(X)

	FLD	 0 * SIZE(Y)
	fmul	%st(1), %st
	faddp	%st, %st(4)

	FMUL	 1 * SIZE(Y)
	faddp	%st, %st(4)
	jmp	.L27
	ALIGN_3

.L14:
	movl	N, %eax
	sarl	$1,   %eax
	jle	.L30
	ALIGN_3

.L31:
	FLD	 0 * SIZE(X)

	FLD	 0 * SIZE(Y)
	fmul	%st(1), %st
	faddp	%st, %st(2)

	FMUL	 1 * SIZE(Y)
	faddp	%st, %st(2)
	FLD	 1 * SIZE(X)

	FLD	 0 * SIZE(Y)
	fmul	%st(1), %st
	faddp	%st, %st(4)

	FMUL	 1 * SIZE(Y)
	faddp	%st, %st(4)
	addl	INCX, X

	FLD	 0 * SIZE(X)
	addl	INCY, Y

	FLD	 0 * SIZE(Y)
	fmul	%st(1), %st
	faddp	%st, %st(2)

	FMUL	 1 * SIZE(Y)
	faddp	%st, %st(2)
	FLD	 1 * SIZE(X)

	FLD	 0 * SIZE(Y)
	fmul	%st(1), %st
	faddp	%st, %st(4)

	FMUL	 1 * SIZE(Y)
	faddp	%st, %st(4)
	addl	INCX, X
	addl	INCY, Y

	decl	%eax
	jg	.L31
	ALIGN_3

.L30:
	movl	N,  %eax
	andl	$1, %eax
	jle	.L27
	ALIGN_3

.L37:
	FLD	 0 * SIZE(X)

	FLD	 0 * SIZE(Y)
	fmul	%st(1), %st
	faddp	%st, %st(2)

	FMUL	 1 * SIZE(Y)
	faddp	%st, %st(2)
	FLD	 1 * SIZE(X)

	FLD	 0 * SIZE(Y)
	fmul	%st(1), %st
	faddp	%st, %st(4)

	FMUL	 1 * SIZE(Y)
	faddp	%st, %st(4)
	ALIGN_3

.L27:
#if defined(DOUBLE) || defined(XDOUBLE)
	movl	RESULT, %eax
#endif

#ifndef CONJ
	fsubp	%st, %st(3)
	faddp	%st, %st(1)
#else
	faddp	%st, %st(3)
	fsubp	%st, %st(1)
#endif

#if !defined(DOUBLE) && !defined(XDOUBLE)
	subl	$2 * SIZE, %esp
	FST	1 * SIZE(%esp)
	FST	0 * SIZE(%esp)
	movl	0 * SIZE(%esp), %eax
	movl	1 * SIZE(%esp), %edx
	addl	$2 * SIZE, %esp
#else
	FST	1 * SIZE(%eax)
	FST	0 * SIZE(%eax)
#endif

	popl	%ebx
	popl	%esi
	popl	%edi
#if defined(DOUBLE) || defined(XDOUBLE)
	ret	$0x4
#else
	ret
#endif
	ALIGN_3

.L88:
#if defined(DOUBLE) || defined(XDOUBLE)
	movl	RESULT, %eax
#endif

	fldz
	fldz

#if !defined(DOUBLE) && !defined(XDOUBLE)
	xor	%eax, %eax
	xor	%edx, %edx
#else
	FST	1 * SIZE(%eax)
	FST	0 * SIZE(%eax)
#endif

	popl	%ebx
	popl	%esi
	popl	%edi

#if defined(DOUBLE) || defined(XDOUBLE)
	ret	$0x4
#else
	ret
#endif
	EPILOGUE
