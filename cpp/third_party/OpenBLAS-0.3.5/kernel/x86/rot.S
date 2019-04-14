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
#define STACK_C		24 + STACK + ARGS(%esp)
#ifdef XDOUBLE
#define STACK_S		40 + STACK + ARGS(%esp)
#elif defined DOUBLE
#define STACK_S		32 + STACK + ARGS(%esp)
#else
#define STACK_S		28 + STACK + ARGS(%esp)
#endif

#define N	%ebx
#define X	%esi
#define INCX	%ecx
#define Y	%edi
#define INCY	%edx

#define	I	%eax

#ifdef PENTIUM4
#define PREFETCH	prefetcht0
#define PREFETCH_SIZE	144
#endif

#ifdef OPTERON
#define PREFETCH	prefetchw
#define PREFETCH_SIZE	144
#endif

	PROLOGUE

	pushl	%edi
	pushl	%esi
	pushl	%ebx

	PROFCODE

#if defined(F_INTERFACE_GFORT) || defined(F_INTERFACE_G95)
	EMMS
#endif

	movl	STACK_N,     N
	movl	STACK_X,     X
	movl	STACK_INCX,  INCX
	movl	STACK_Y,     Y
	movl	STACK_INCY,  INCY

	FLD	STACK_S
	FLD	STACK_C

	sall	$BASE_SHIFT, INCX
	sall	$BASE_SHIFT, INCY

	testl	N, N
	jle	.L999

	cmpl	$SIZE, INCX
	jne	.L50
	cmpl	$SIZE, INCY
	jne	.L50

	movl	N,  I
	sarl	$2, I
	jle	.L15
	ALIGN_4

.L10:
#ifdef PENTIUM4
	PREFETCH	(PREFETCH_SIZE + 0) * SIZE(X)
#endif
#ifdef OPTERON
	PREFETCH	(PREFETCH_SIZE + 0) * SIZE(X)
#endif

	FLD	0 * SIZE(X)
	FLD	0 * SIZE(Y)

	fld	%st(1)
	fmul	%st(3), %st

	fld	%st(1)
	fmul	%st(5), %st

	faddp	%st, %st(1)
	FST	0 * SIZE(X)

	fmul	%st(2), %st
	fxch	%st(1)
	fmul	%st(3), %st

	fsubrp	%st, %st(1)
	FST	0 * SIZE(Y)

	FLD	1 * SIZE(X)
	FLD	1 * SIZE(Y)

	fld	%st(1)
	fmul	%st(3), %st

	fld	%st(1)
	fmul	%st(5), %st

	faddp	%st, %st(1)
	FST	1 * SIZE(X)

	fmul	%st(2), %st
	fxch	%st(1)
	fmul	%st(3), %st

	fsubrp	%st, %st(1)
	FST	1 * SIZE(Y)

#ifdef PENTIUM4
	PREFETCH	(PREFETCH_SIZE + 0) * SIZE(Y)
#endif
#ifdef OPTERON
	PREFETCH	(PREFETCH_SIZE + 0) * SIZE(Y)
#endif

	FLD	2 * SIZE(X)
	FLD	2 * SIZE(Y)

	fld	%st(1)
	fmul	%st(3), %st

	fld	%st(1)
	fmul	%st(5), %st

	faddp	%st, %st(1)
	FST	2 * SIZE(X)

	fmul	%st(2), %st
	fxch	%st(1)
	fmul	%st(3), %st

	fsubrp	%st, %st(1)
	FST	2 * SIZE(Y)

	FLD	3 * SIZE(X)
	FLD	3 * SIZE(Y)

	fld	%st(1)
	fmul	%st(3), %st

	fld	%st(1)
	fmul	%st(5), %st

	faddp	%st, %st(1)
	FST	3 * SIZE(X)

	fmul	%st(2), %st
	fxch	%st(1)
	fmul	%st(3), %st

	fsubrp	%st, %st(1)
	FST	3 * SIZE(Y)

	addl	$4 * SIZE, X
	addl	$4 * SIZE, Y

	decl	I
	jg	.L10
	ALIGN_4

.L15:
	movl	N,  I
	andl	$3, I
	jle	.L999
	ALIGN_4

.L16:
	FLD	0 * SIZE(X)
	FLD	0 * SIZE(Y)

	fld	%st(1)
	fmul	%st(3), %st

	fld	%st(1)
	fmul	%st(5), %st

	faddp	%st, %st(1)
	FST	0 * SIZE(X)

	fmul	%st(2), %st
	fxch	%st(1)
	fmul	%st(3), %st

	fsubrp	%st, %st(1)
	FST	0 * SIZE(Y)

	addl	$SIZE, X
	addl	$SIZE, Y

	decl	I
	jg	.L16
	jmp	.L999
	ALIGN_4

.L50:
	movl	N,  I
	sarl	$2, I
	jle	.L55
	ALIGN_4

.L51:
	FLD	0 * SIZE(X)
	FLD	0 * SIZE(Y)

	fld	%st(1)
	fmul	%st(3), %st

	fld	%st(1)
	fmul	%st(5), %st

	faddp	%st, %st(1)
	FST	0 * SIZE(X)

	fmul	%st(2), %st
	fxch	%st(1)
	fmul	%st(3), %st

	fsubrp	%st, %st(1)
	FST	0 * SIZE(Y)

	addl	INCX, X
	addl	INCY, Y

	FLD	0 * SIZE(X)
	FLD	0 * SIZE(Y)

	fld	%st(1)
	fmul	%st(3), %st

	fld	%st(1)
	fmul	%st(5), %st

	faddp	%st, %st(1)
	FST	0 * SIZE(X)

	fmul	%st(2), %st
	fxch	%st(1)
	fmul	%st(3), %st

	fsubrp	%st, %st(1)
	FST	0 * SIZE(Y)

	addl	INCX, X
	addl	INCY, Y

	FLD	0 * SIZE(X)
	FLD	0 * SIZE(Y)

	fld	%st(1)
	fmul	%st(3), %st

	fld	%st(1)
	fmul	%st(5), %st

	faddp	%st, %st(1)
	FST	0 * SIZE(X)

	fmul	%st(2), %st
	fxch	%st(1)
	fmul	%st(3), %st

	fsubrp	%st, %st(1)
	FST	0 * SIZE(Y)

	addl	INCX, X
	addl	INCY, Y

	FLD	0 * SIZE(X)
	FLD	0 * SIZE(Y)

	fld	%st(1)
	fmul	%st(3), %st

	fld	%st(1)
	fmul	%st(5), %st

	faddp	%st, %st(1)
	FST	0 * SIZE(X)

	fmul	%st(2), %st
	fxch	%st(1)
	fmul	%st(3), %st

	fsubrp	%st, %st(1)
	FST	0 * SIZE(Y)

	addl	INCX, X
	addl	INCY, Y

	decl	I
	jg	.L51
	ALIGN_4

.L55:
	movl	N,  I
	andl	$3, I
	jle	.L999
	ALIGN_4

.L56:
	FLD	0 * SIZE(X)
	FLD	0 * SIZE(Y)

	fld	%st(1)
	fmul	%st(3), %st

	fld	%st(1)
	fmul	%st(5), %st

	faddp	%st, %st(1)
	FST	0 * SIZE(X)

	fmul	%st(2), %st
	fxch	%st(1)
	fmul	%st(3), %st

	fsubrp	%st, %st(1)
	FST	0 * SIZE(Y)

	addl	INCX, X
	addl	INCY, Y

	decl	I
	jg	.L56
	ALIGN_4


.L999:
	ffreep	%st(0)
	ffreep	%st(0)

	popl	%ebx
	popl	%esi
	popl	%edi
	ret

	EPILOGUE
