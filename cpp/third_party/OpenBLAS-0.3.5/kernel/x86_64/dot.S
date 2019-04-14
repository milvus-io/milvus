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

#define N	ARG1	/* rdi */
#define X	ARG2	/* rsi */
#define INCX	ARG3	/* rdx */
#define Y	ARG4	/* rcx */
#define INCY	ARG5	/* r8  */

#include "l1param.h"

	PROLOGUE
	PROFCODE

	salq	$BASE_SHIFT, INCX
	salq	$BASE_SHIFT, INCY

	fldz
	fldz
	fldz
	fldz

	cmpq	$SIZE, INCX
	jne	.L14
	cmpq	$SIZE, INCY
	jne	.L14

	movq	N, %rax
	sarq	$2,   %rax
	jle	.L15
	ALIGN_3

.L16:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	FLD	0 * SIZE(X)
	FLD	0 * SIZE(Y)
	fmulp	%st, %st(1)
	faddp	%st,%st(1)
	FLD	1 * SIZE(X)
	FLD	1 * SIZE(Y)
	fmulp	%st, %st(1)
	faddp	%st,%st(2)

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	FLD	2 * SIZE(X)
	FLD	2 * SIZE(Y)
	fmulp	%st, %st(1)
	faddp	%st,%st(3)
	FLD	3 * SIZE(X)
	FLD	3 * SIZE(Y)
	fmulp	%st, %st(1)
	faddp	%st,%st(4)
	addq	$4 * SIZE, X
	addq	$4 * SIZE, Y
	decq	%rax
	jg	.L16
	ALIGN_3

.L15:
	movq	N, %rax
	andq	$3,   %rax
	jle	.L27
	ALIGN_3

.L22:
	FLD	(X)
	addq	$SIZE, X
	FLD	(Y)
	fmulp	%st, %st(1)
	addq	$SIZE, Y
	faddp	%st,%st(1)
	decq	%rax
	jg	.L22

	jmp	.L27
	ALIGN_3

.L14:
	movq	N, %rax
	sarq	$2,   %rax
	jle	.L30
	ALIGN_3

.L31:
	FLD	(X)
	addq	INCX, X
	FLD	(Y)
	fmulp	%st, %st(1)
	addq	INCY, Y
	faddp	%st,%st(1)

	FLD	(X)
	addq	INCX, X
	FLD	(Y)
	fmulp	%st, %st(1)
	addq	INCY, Y
	faddp	%st,%st(2)

	FLD	(X)
	addq	INCX, X
	FLD	(Y)
	fmulp	%st, %st(1)
	addq	INCY, Y
	faddp	%st,%st(3)

	FLD	(X)
	addq	INCX, X
	FLD	(Y)
	fmulp	%st, %st(1)
	addq	INCY, Y
	faddp	%st,%st(4)

	decq	%rax
	jg	.L31
	ALIGN_3

.L30:
	movq	N, %rax
	andq	$3,   %rax
	jle	.L27
	ALIGN_3

.L37:
	FLD	(X)
	addq	INCX, X
	FLD	(Y)
	fmulp	%st, %st(1)
	addq	INCY, Y
	faddp	%st, %st(1)
	decq	%rax
	jg	.L37
	ALIGN_3

.L27:
	faddp	%st,%st(2)
	faddp	%st,%st(2)
	faddp	%st,%st(1)
	ret

	EPILOGUE
