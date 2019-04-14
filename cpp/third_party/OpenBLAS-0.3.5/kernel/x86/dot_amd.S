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

	PROLOGUE

	pushl	%edi
	pushl	%esi
	pushl	%ebx

	PROFCODE

#if defined(F_INTERFACE_GFORT) || defined(F_INTERFACE_G95)
	EMMS
#endif

#define N	%ebx
#define X	%esi
#define INCX	%ecx
#define Y	%edi
#define INCY	%edx

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

	leal	(, INCX, SIZE), INCX
	leal	(, INCY, SIZE), INCY

	fldz
	fldz
	fldz
	fldz

	cmpl	$SIZE, INCX
	jne	.L14
	cmpl	$SIZE, INCY
	jne	.L14

	movl	N,  %eax
	sarl	$3, %eax
	jle	.L15
	FLD	0 * SIZE(X)
	ALIGN_4

.L16:
	FLD	1 * SIZE(X)
	FMUL	1 * SIZE(Y)
	faddp	%st,%st(2)
	FMUL	0 * SIZE(Y)
	faddp	%st,%st(2)
	FLD	2 * SIZE(X)
	FLD	3 * SIZE(X)
	FMUL	3 * SIZE(Y)
	faddp	%st,%st(4)
	FMUL	2 * SIZE(Y)
	faddp	%st,%st(4)
	FLD	4 * SIZE(X)

	FLD	5 * SIZE(X)
	FMUL	5 * SIZE(Y)
	faddp	%st,%st(2)
	FMUL	4 * SIZE(Y)
	faddp	%st,%st(2)
	FLD	6 * SIZE(X)
	FLD	7 * SIZE(X)
	FMUL	7 * SIZE(Y)
	faddp	%st,%st(4)
	FMUL	6 * SIZE(Y)
	faddp	%st,%st(4)
	FLD	8 * SIZE(X)

	prefetch  16 * SIZE(X)
	addl	$8 * SIZE, X
	addl	$8 * SIZE, Y
	decl	%eax
	jg	.L16

	ffreep	%st(0)
	ALIGN_3

.L15:
	movl	N,  %eax
	andl	$7, %eax
	jle	.L27
	ALIGN_3

.L22:
	FLD	(X)
	addl	$SIZE, X
	FMUL	(Y)
	addl	$SIZE, Y
	faddp	%st,%st(1)
	decl	%eax
	jg	.L22

	jmp	.L27
	ALIGN_3

.L14:
#ifdef F_INTERFACE
	testl	INCX, INCX
	jge	.L28

	movl	N, %eax
	decl	%eax
	imull	INCX, %eax
	subl	%eax, X
	ALIGN_3

.L28:
	testl	INCY, INCY
	jge	.L29

	movl	N, %eax
	decl	%eax
	imull	INCY, %eax
	subl	%eax, Y
	ALIGN_3
.L29:
#endif
	movl	N, %eax
	sarl	$2,   %eax
	jle	.L30
	ALIGN_3

.L31:
	FLD	(X)
	addl	INCX, X
	FMUL	(Y)
	addl	INCY, Y
	faddp	%st,%st(1)

	FLD	(X)
	addl	INCX, X
	FMUL	(Y)
	addl	INCY, Y
	faddp	%st,%st(2)

	FLD	(X)
	addl	INCX, X
	FMUL	(Y)
	addl	INCY, Y
	faddp	%st,%st(3)

	FLD	(X)
	addl	INCX, X
	FMUL	(Y)
	addl	INCY, Y
	faddp	%st,%st(4)

	decl	%eax
	jg	.L31
	ALIGN_3

.L30:
	movl	N, %eax
	andl	$3,   %eax
	jle	.L27
	ALIGN_3

.L37:
	FLD	(X)
	addl	INCX, X
	FMUL	(Y)
	addl	INCY, Y
	faddp	%st, %st(1)
	decl	%eax
	jg	.L37
	ALIGN_3

.L27:
	faddp	%st,%st(2)
	faddp	%st,%st(2)
	faddp	%st,%st(1)

	popl	%ebx
	popl	%esi
	popl	%edi
	ret

	EPILOGUE
