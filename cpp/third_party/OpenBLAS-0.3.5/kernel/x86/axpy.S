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
#define STACK_ALPHA	16 + STACK + ARGS(%esp)
#ifdef DOUBLE
#define STACK_X		24 + STACK + ARGS(%esp)
#define STACK_INCX	28 + STACK + ARGS(%esp)
#define STACK_Y		32 + STACK + ARGS(%esp)
#define STACK_INCY	36 + STACK + ARGS(%esp)
#else
#define STACK_X		20 + STACK + ARGS(%esp)
#define STACK_INCX	24 + STACK + ARGS(%esp)
#define STACK_Y		28 + STACK + ARGS(%esp)
#define STACK_INCY	32 + STACK + ARGS(%esp)
#endif

#define M	%ebx
#define X	%esi
#define INCX	%ecx
#define Y	%edi
#define INCY	%edx


	PROLOGUE

	pushl	%edi
	pushl	%esi
	pushl	%ebx

	PROFCODE

#if defined(F_INTERFACE_GFORT) || defined(F_INTERFACE_G95)
	EMMS
#endif

	FLD	STACK_ALPHA

	movl	STACK_M,    M
	movl	STACK_X,    X
	movl	STACK_INCX, INCX
	movl	STACK_Y,    Y
	movl	STACK_INCY, INCY

	leal	(, INCX, SIZE), INCX
	leal	(, INCY, SIZE), INCY

	testl	M, M
	jle	.L40

	cmpl	$SIZE, INCX
	jne	.L14
	cmpl	$SIZE, INCY
	jne	.L14

	movl	M, %eax
	sarl	$3,   %eax
	jle	.L15
	ALIGN_3

#define PRESIZE 33

.L16:
#ifdef HAS_PREFETCH
	prefetcht0	PRESIZE * SIZE(X)
#endif

	FLD	0 * SIZE(X)
	fmul	%st(1),%st
	FADD	0 * SIZE(Y)
	FST	0 * SIZE(Y)

	FLD	1 * SIZE(X)
	fmul	%st(1),%st
	FADD	1 * SIZE(Y)
	FST	1 * SIZE(Y)

	FLD	2 * SIZE(X)
	fmul	%st(1),%st
	FADD	2 * SIZE(Y)
	FST	2 * SIZE(Y)

	FLD	3 * SIZE(X)
	fmul	%st(1),%st
	FADD	3 * SIZE(Y)
	FST	3 * SIZE(Y)

#ifdef HAS_PREFETCH
	prefetcht0	(4 + PRESIZE) * SIZE(X)
#endif

	FLD	4 * SIZE(X)
	fmul	%st(1),%st
	FADD	4 * SIZE(Y)
	FST	4 * SIZE(Y)

	FLD	5 * SIZE(X)
	fmul	%st(1),%st
	FADD	5 * SIZE(Y)
	FST	5 * SIZE(Y)

	FLD	6 * SIZE(X)
	fmul	%st(1),%st
	FADD	6 * SIZE(Y)
	FST	6 * SIZE(Y)

	FLD	7 * SIZE(X)
	fmul	%st(1),%st
	FADD	7 * SIZE(Y)
	FST	7 * SIZE(Y)

#ifdef HAVE_3DNOW
	prefetchw	24 * SIZE(Y)
#endif

	addl	$8 * SIZE, X
	addl	$8 * SIZE, Y
	decl	%eax
	jg	.L16
	ALIGN_3

.L15:
	movl	M, %eax
	andl	$7,   %eax
	jle	.L40
	ALIGN_3

.L22:
	FLD	0 * SIZE(X)
	fmul	%st(1),%st
	FADD	0 * SIZE(Y)
	FST	0 * SIZE(Y)
	addl	$SIZE, X
	addl	$SIZE, Y
	decl	%eax
	jg	.L22
	jmp	.L40
	ALIGN_3

.L14:
	movl	M, %eax
	sarl	$2,   %eax
	jle	.L28
	ALIGN_3

.L29:
	FLD	(X)
	fmul	%st(1),%st
	FADD	(Y)
	FST	(Y)
	addl	INCX, X
	addl	INCY, Y

	FLD	(X)
	fmul	%st(1),%st
	FADD	(Y)
	FST	(Y)
	addl	INCX, X
	addl	INCY, Y

	FLD	(X)
	fmul	%st(1),%st
	FADD	(Y)
	FST	(Y)
	addl	INCX, X
	addl	INCY, Y

	FLD	(X)
	fmul	%st(1),%st
	FADD	(Y)
	FST	(Y)
	addl	INCX, X
	addl	INCY, Y

	decl	%eax
	jg	.L29
	ALIGN_3

.L28:
	movl	M, %eax
	andl	$3,   %eax
	jle	.L40
	ALIGN_3

.L35:
	FLD	(X)
	fmul	%st(1),%st
	FADD	(Y)
	FST	(Y)
	addl	INCX, X
	addl	INCY, Y

	decl	%eax
	jg	.L35
	ALIGN_3

.L40:
	ffreep	%st(0)
	xorl	%eax,%eax

	popl	%ebx
	popl	%esi
	popl	%edi
	ret

	EPILOGUE
