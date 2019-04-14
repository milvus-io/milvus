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
#define X	ARG4
#define INCX	ARG5

#define I	%rax

#include "l1param.h"

	PROLOGUE
	PROFCODE

	salq	$ZBASE_SHIFT, INCX

	FLD	 8(%rsp)
	FLD	24(%rsp)

	testq	N, N
	jle	.L999

	fld	%st(1)
	fabs
	fld	%st(1)
	fabs
	faddp	%st, %st(1)

	fldz
	fcomip	%st(1), %st
	ffreep	%st
	jne	.L30

	EMMS

	pxor	%mm0, %mm0

	cmpq	$2 * SIZE, INCX
	jne	.L20

	movq	N,  I
	sarq	$2, I
	jle	.L15
	ALIGN_4

.L12:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movq	%mm0,   0(X)
	movq	%mm0,   8(X)
	movq	%mm0,  16(X)
	movq	%mm0,  24(X)
	movq	%mm0,  32(X)
	movq	%mm0,  40(X)
	movq	%mm0,  48(X)
	movq	%mm0,  56(X)
	movq	%mm0,  64(X)
	movq	%mm0,  72(X)
	movq	%mm0,  80(X)
	movq	%mm0,  88(X)
	movq	%mm0,  96(X)
	movq	%mm0, 104(X)
	movq	%mm0, 112(X)
	movq	%mm0, 120(X)
	addq	$8 * SIZE, X
	decq	I
	jg	.L12
	ALIGN_3

.L15:
	movq	N,  I
	andq	$3, I
	jle	.L18
	ALIGN_2

.L16:
	movq	%mm0,   0(X)
	movq	%mm0,   8(X)
	movq	%mm0,  16(X)
	movq	%mm0,  24(X)

	addq	$2 * SIZE, X
	decq	I
	jg	.L16

.L18:
	EMMS

	ret
	ALIGN_2

.L20:
	movq	N,  I
	sarq	$2, I
	jle	.L25
	ALIGN_3

.L22:
	movq	%mm0,   0(X)
	movq	%mm0,   8(X)
	movq	%mm0,  16(X)
	movq	%mm0,  24(X)
	addq	INCX, X

	movq	%mm0,   0(X)
	movq	%mm0,   8(X)
	movq	%mm0,  16(X)
	movq	%mm0,  24(X)
	addq	INCX, X

	movq	%mm0,   0(X)
	movq	%mm0,   8(X)
	movq	%mm0,  16(X)
	movq	%mm0,  24(X)
	addq	INCX, X

	movq	%mm0,   0(X)
	movq	%mm0,   8(X)
	movq	%mm0,  16(X)
	movq	%mm0,  24(X)
	addq	INCX, X

	decq	I
	jg	.L22
	ALIGN_3

.L25:
	movq	N,  I
	andq	$3, I
	jle	.L28
	ALIGN_3

.L26:
	movq	%mm0,   0(X)
	movq	%mm0,   8(X)
	movq	%mm0,  16(X)
	movq	%mm0,  24(X)
	addq	INCX, X

	decq	I
	jg	.L26

.L28:
	EMMS

	ret
	ALIGN_3

.L30:
	movq	N, I
	ALIGN_2

.L32:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	FLD	0 * SIZE(X)
	fmul	%st(1),%st
	FLD	1 * SIZE(X)
	fmul	%st(3),%st
	faddp	%st,%st(1)

	FLD	0 * SIZE(X)
	fmul	%st(3),%st
	FLD	1 * SIZE(X)
	fmul	%st(3),%st
	fsubrp	%st,%st(1)

	FST	0 * SIZE(X)
	FST	1 * SIZE(X)
	addq	INCX, X
	decq	I
	jg	.L32
	ALIGN_2

.L999:
	ffreep	%st
	ffreep	%st

	ret

	EPILOGUE
