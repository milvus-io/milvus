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

#define M	ARG1
#define X	ARG2
#define INCX	ARG3

#define I	ARG4
#define NUM	%r10
#define RET	%rax

#ifndef USE_MIN
#define FMOV	fcmovbe
#define IMOV	cmovnbe
#else
#define FMOV	fcmovnb
#define IMOV	cmovb
#endif

#include "l1param.h"

	PROLOGUE
	PROFCODE

	salq	$ZBASE_SHIFT, INCX

	fldz
	xorq	RET, RET

	testq	M, M
	jle	.L999
	testq	INCX, INCX
	jle	.L999

	ffreep	%st
	movq	$2, NUM
	movq	$1, RET

	FLD	0 * SIZE(X)
	fabs
	FLD	1 * SIZE(X)
	fabs
	faddp	%st, %st(1)
	addq	INCX, X
	decq	M
	jle	.L999

	cmpq	$2 * SIZE, INCX
	jne	.L40

	movq	M,  I
	sarq	$2, I
	jle	.L20
	ALIGN_4

.L10:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	FLD	0 * SIZE(X)
	fabs
	FLD	1 * SIZE(X)
	fabs
	faddp	%st, %st(1)
	fcomi
	FMOV	%st(1), %st(0)
	IMOV	NUM, RET
	fxch	%st(1)
	ffreep	%st
	incq	NUM

	FLD	2 * SIZE(X)
	fabs
	FLD	3 * SIZE(X)
	fabs
	faddp	%st, %st(1)
	fcomi
	FMOV	%st(1), %st(0)
	IMOV	NUM, RET
	fxch	%st(1)
	ffreep	%st
	incq	NUM

	FLD	4 * SIZE(X)
	fabs
	FLD	5 * SIZE(X)
	fabs
	faddp	%st, %st(1)
	fcomi
	FMOV	%st(1), %st(0)
	IMOV	NUM, RET
	fxch	%st(1)
	ffreep	%st
	incq	NUM

	FLD	6 * SIZE(X)
	fabs
	FLD	7 * SIZE(X)
	fabs
	faddp	%st, %st(1)
	fcomi
	FMOV	%st(1), %st(0)
	IMOV	NUM, RET
	fxch	%st(1)
	ffreep	%st
	incq	NUM

	addq	$8 * SIZE, X

	decq	I
	jg	.L10
	ALIGN_4

.L20:
	movq	M,  I
	andq	$3, I
	jle	.L999
	ALIGN_4

.L21:
	FLD	0 * SIZE(X)
	fabs
	FLD	1 * SIZE(X)
	fabs
	faddp	%st, %st(1)
	fcomi
	FMOV	%st(1), %st(0)
	IMOV	NUM, RET
	fxch	%st(1)
	ffreep	%st
	incq	NUM

	addq	$2 * SIZE, X
	decq	I
	jg	.L21
	jmp	.L999
	ALIGN_4

.L40:
	movq	M,  I
	sarq	$2, I
	jle	.L60
	ALIGN_4

.L50:
	FLD	0 * SIZE(X)
	fabs
	FLD	1 * SIZE(X)
	fabs
	addq	INCX, X
	faddp	%st, %st(1)
	fcomi
	FMOV	%st(1), %st(0)
	IMOV	NUM, RET
	fxch	%st(1)
	ffreep	%st
	incq	NUM

	FLD	0 * SIZE(X)
	fabs
	FLD	1 * SIZE(X)
	fabs
	addq	INCX, X
	faddp	%st, %st(1)
	fcomi
	FMOV	%st(1), %st(0)
	IMOV	NUM, RET
	fxch	%st(1)
	ffreep	%st
	incq	NUM

	FLD	0 * SIZE(X)
	fabs
	FLD	1 * SIZE(X)
	fabs
	addq	INCX, X
	faddp	%st, %st(1)
	fcomi
	FMOV	%st(1), %st(0)
	IMOV	NUM, RET
	fxch	%st(1)
	ffreep	%st
	incq	NUM

	FLD	0 * SIZE(X)
	fabs
	FLD	1 * SIZE(X)
	fabs
	addq	INCX, X
	faddp	%st, %st(1)
	fcomi
	FMOV	%st(1), %st(0)
	IMOV	NUM, RET
	fxch	%st(1)
	ffreep	%st
	incq	NUM

	decq	I
	jg	.L50
	ALIGN_4

.L60:
	movq	M,  I
	andq	$3, I
	jle	.L999
	ALIGN_4


.L61:
	FLD	0 * SIZE(X)
	fabs
	FLD	1 * SIZE(X)
	fabs
	faddp	%st, %st(1)
	fcomi
	FMOV	%st(1), %st(0)
	IMOV	NUM, RET
	fxch	%st(1)
	ffreep	%st
	incq	NUM

	addq	INCX, X
	decq	I
	jg	.L61
	ALIGN_4

.L999:
	ffreep	%st
	ret

	EPILOGUE
