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

#define STACK	 8
#define ARGS	 0

#define STACK_M		 4 + STACK + ARGS(%esp)
#define STACK_X		 8 + STACK + ARGS(%esp)
#define STACK_INCX	12 + STACK + ARGS(%esp)

#define M	%edx
#define X	%ecx
#define INCX	%esi

#define I	%eax

#include "l1param.h"

	PROLOGUE

	pushl	%esi
	pushl	%ebx

	PROFCODE

#if defined(F_INTERFACE_GFORT) || defined(F_INTERFACE_G95)
	EMMS
#endif

	movl	STACK_M,    M
	movl	STACK_X,    X
	movl	STACK_INCX, INCX

#ifdef F_INTERFACE
	movl	(M),    M
	movl	(INCX), INCX
#endif

	fldz
	testl	M, M
	jle	.L999
	testl	INCX, INCX
	jle	.L999

	sall	$BASE_SHIFT, INCX
	fldz
	fldz
	fldz
	cmpl	$SIZE, INCX
	jne	.L40

	movl	M,  I
	sarl	$3, I
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
	FLD	2 * SIZE(X)
	fabs
	FLD	3 * SIZE(X)
	fabs

	faddp	%st, %st(7)
	faddp	%st, %st(5)
	faddp	%st, %st(3)
	faddp	%st, %st(1)

	FLD	4 * SIZE(X)
	fabs
	FLD	5 * SIZE(X)
	fabs
	FLD	6 * SIZE(X)
	fabs
	FLD	7 * SIZE(X)
	fabs

	addl	$8 * SIZE, X

	faddp	%st, %st(7)
	faddp	%st, %st(5)
	faddp	%st, %st(3)
	faddp	%st, %st(1)

	decl	I
	jg	.L10
	ALIGN_4

.L20:
	movl	M,  I
	andl	$7, I
	jle	.L998
	ALIGN_4


.L21:
	FLD	(X)
	fabs
	faddp	%st,%st(1)
	addl	$1 * SIZE, X
	decl	I
	jg	.L21
	jmp	.L998
	ALIGN_4

.L40:
	movl	M,  I
	sarl	$3, I
	jle	.L60
	ALIGN_4

.L50:
	FLD	(X)
	addl	INCX, X
	fabs
	FLD	(X)
	addl	INCX, X
	fabs
	FLD	(X)
	addl	INCX, X
	fabs
	FLD	(X)
	addl	INCX, X
	fabs

	faddp	%st, %st(7)
	faddp	%st, %st(5)
	faddp	%st, %st(3)
	faddp	%st, %st(1)

	FLD	(X)
	addl	INCX, X
	fabs
	FLD	(X)
	addl	INCX, X
	fabs
	FLD	(X)
	addl	INCX, X
	fabs
	FLD	(X)
	addl	INCX, X
	fabs

	faddp	%st, %st(7)
	faddp	%st, %st(5)
	faddp	%st, %st(3)
	faddp	%st, %st(1)

	decl	I
	jg	.L50
	ALIGN_4

.L60:
	movl	M,  I
	andl	$7, I
	jle	.L998
	ALIGN_4


.L61:
	FLD	(X)
	addl	INCX, X
	fabs
	faddp	%st,%st(1)
	decl	I
	jg	.L61
	ALIGN_4

.L998:
	faddp	%st,%st(2)
	faddp	%st,%st(1)
	faddp	%st,%st(1)
	ALIGN_4

.L999:
	popl	%ebx
	popl	%esi
	ret

	EPILOGUE
