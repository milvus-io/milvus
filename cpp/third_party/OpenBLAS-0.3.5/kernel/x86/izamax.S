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

#define STACK	16
#define ARGS	 0

#define STACK_M		 4 + STACK + ARGS(%esp)
#define STACK_X	 	 8 + STACK + ARGS(%esp)
#define STACK_INCX	12 + STACK + ARGS(%esp)

	PROLOGUE

#define M	%ebx
#define INCX	%esi
#define X	%ecx
#define I	%edx
#define NUM	%edi
#define RET	%eax

#ifndef USE_MIN
#define FMOV	fcmovbe
#define IMOV	cmovnbe
#else
#define FMOV	fcmovnb
#define IMOV	cmovb
#endif

#include "l1param.h"

	pushl	%ebp
	pushl	%edi
	pushl	%esi
	pushl	%ebx

	PROFCODE

	movl	STACK_M,    M
	movl	STACK_INCX, INCX
	movl	STACK_X,    X

#ifdef F_INTERFACE
	movl	(M), M
	movl	(INCX), INCX
#endif

#if defined(F_INTERFACE_GFORT) || defined(F_INTERFACE_G95)
	EMMS
#endif
	sall	$ZBASE_SHIFT, INCX

	fldz
	xorl	RET, RET

	testl	M, M
	jle	.L999
	testl	INCX, INCX
	jle	.L999

	fstp	%st(0)
	movl	$2, NUM
	movl	$1, RET

	FLD	0 * SIZE(X)
	fabs
	FLD	1 * SIZE(X)
	fabs
	faddp	%st, %st(1)
	addl	INCX, X
	decl	M
	jle	.L999

	cmpl	$2 * SIZE, INCX
	jne	.L40

	movl	M,  I
	sarl	$2, I
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
	fcomi	%st(1), %st
	FMOV	%st(1), %st(0)
	IMOV	NUM, RET
	fstp	%st(1)
	incl	NUM

	FLD	2 * SIZE(X)
	fabs
	FLD	3 * SIZE(X)
	fabs
	faddp	%st, %st(1)
	fcomi	%st(1), %st
	FMOV	%st(1), %st(0)
	IMOV	NUM, RET
	fstp	%st(1)
	incl	NUM

	FLD	4 * SIZE(X)
	fabs
	FLD	5 * SIZE(X)
	fabs
	faddp	%st, %st(1)
	fcomi	%st(1), %st
	FMOV	%st(1), %st(0)
	IMOV	NUM, RET
	fstp	%st(1)
	incl	NUM

	FLD	6 * SIZE(X)
	fabs
	FLD	7 * SIZE(X)
	fabs
	faddp	%st, %st(1)
	fcomi	%st(1), %st
	FMOV	%st(1), %st(0)
	IMOV	NUM, RET
	fstp	%st(1)
	incl	NUM

	addl	$8 * SIZE, X

	decl	I
	jg	.L10
	ALIGN_4

.L20:
	movl	M,  I
	andl	$3, I
	jle	.L999
	ALIGN_4

.L21:
	FLD	0 * SIZE(X)
	fabs
	FLD	1 * SIZE(X)
	fabs
	faddp	%st, %st(1)
	fcomi	%st(1), %st
	FMOV	%st(1), %st(0)
	IMOV	NUM, RET
	fstp	%st(1)
	incl	NUM

	addl	$2 * SIZE, X
	decl	I
	jg	.L21
	jmp	.L999
	ALIGN_4

.L40:
	movl	M,  I
	sarl	$2, I
	jle	.L60
	ALIGN_4

.L50:
	FLD	0 * SIZE(X)
	fabs
	FLD	1 * SIZE(X)
	fabs
	addl	INCX, X
	faddp	%st, %st(1)
	fcomi	%st(1), %st
	FMOV	%st(1), %st(0)
	IMOV	NUM, RET
	fstp	%st(1)
	incl	NUM

	FLD	0 * SIZE(X)
	fabs
	FLD	1 * SIZE(X)
	fabs
	addl	INCX, X
	faddp	%st, %st(1)
	fcomi	%st(1), %st
	FMOV	%st(1), %st(0)
	IMOV	NUM, RET
	fstp	%st(1)
	incl	NUM

	FLD	0 * SIZE(X)
	fabs
	FLD	1 * SIZE(X)
	fabs
	addl	INCX, X
	faddp	%st, %st(1)
	fcomi	%st(1), %st
	FMOV	%st(1), %st(0)
	IMOV	NUM, RET
	fstp	%st(1)
	incl	NUM

	FLD	0 * SIZE(X)
	fabs
	FLD	1 * SIZE(X)
	fabs
	addl	INCX, X
	faddp	%st, %st(1)
	fcomi	%st(1), %st
	FMOV	%st(1), %st(0)
	IMOV	NUM, RET
	fstp	%st(1)
	incl	NUM

	decl	I
	jg	.L50
	ALIGN_4

.L60:
	movl	M,  I
	andl	$3, I
	jle	.L999
	ALIGN_4


.L61:
	FLD	0 * SIZE(X)
	fabs
	FLD	1 * SIZE(X)
	fabs
	faddp	%st, %st(1)
	fcomi	%st(1), %st
	FMOV	%st(1), %st(0)
	IMOV	NUM, RET
	fstp	%st(1)
	incl	NUM

	addl	INCX, X
	decl	I
	jg	.L61
	ALIGN_4

.L999:
	fstp	%st(0)

	popl	%ebx
	popl	%esi
	popl	%edi
	popl	%ebp
	ret

	EPILOGUE
