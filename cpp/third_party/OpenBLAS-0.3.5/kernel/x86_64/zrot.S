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

#define N	ARG1
#define X	ARG2
#define INCX	ARG3
#define Y	ARG4
#ifndef WINDOWS_ABI
#define INCY	ARG5	/* r8  */
#else
#define INCY	%r10
#endif

#define	I	%rax

#include "l1param.h"

	PROLOGUE
	PROFCODE

#ifdef WINDOWS_ABI
	movq	48(%rsp), INCY
	FLD	72(%rsp)
	FLD	56(%rsp)
#else
	FLD	24(%rsp)
	FLD	 8(%rsp)
#endif

	salq	$ZBASE_SHIFT, INCX
	salq	$ZBASE_SHIFT, INCY

	testq	N, N
	jle	.L999

	cmpq	$2 * SIZE, INCX
	jne	.L50
	cmpq	$2 * SIZE, INCY
	jne	.L50

	movq	N,  I
	sarq	$1, I
	jle	.L15
	ALIGN_4

.L10:
#if defined(PREFETCHW)
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
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

#if defined(PREFETCHW)
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
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

	addq	$4 * SIZE, X
	addq	$4 * SIZE, Y

	decq	I
	jg	.L10
	ALIGN_4

.L15:
	movq	N,  I
	andq	$1, I
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
	jmp	.L999
	ALIGN_4

.L50:
	movq	N,  I
	sarq	$1, I
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

	addq	INCX, X
	addq	INCY, Y

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

	addq	INCX, X
	addq	INCY, Y

	decq	I
	jg	.L51
	ALIGN_4

.L55:
	movq	N,  I
	andq	$1, I
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
	ALIGN_4

.L999:
	ffreep	%st
	ffreep	%st
	ret

	EPILOGUE
