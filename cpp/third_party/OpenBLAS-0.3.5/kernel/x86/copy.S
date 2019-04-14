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

#define M	 4 + STACK + ARGS(%esp)
#define X	 8 + STACK + ARGS(%esp)
#define INCX	12 + STACK + ARGS(%esp)
#define Y	16 + STACK + ARGS(%esp)
#define INCY	20 + STACK + ARGS(%esp)

	PROLOGUE

	pushl	%edi
	pushl	%esi
	pushl	%ebx

	PROFCODE

#if defined(F_INTERFACE_GFORT) || defined(F_INTERFACE_G95)
	EMMS
#endif

	movl	M,    %ebx
	movl	X,    %ecx
	movl	INCX, %esi
	movl	Y,    %edx
	movl	INCY, %edi

	testl	%ebx, %ebx	# if m == 0 goto End
	jle	.L999

#if SIZE > 8
	sall	$BASE_SHIFT, %esi
	sall	$BASE_SHIFT, %edi
#else
	leal	(, %esi, SIZE), %esi
	leal	(, %edi, SIZE), %edi
#endif

	cmpl	$SIZE, %esi	# if incx != 1
	jne	.L100
	cmpl	$SIZE, %edi	# if incy != 1
	jne	.L100

	movl	%ebx, %eax	# i = m
	sarl	$3,   %eax
	jle	.L20
	ALIGN_2

.L11:
	FLD	7 * SIZE(%ecx)
	FLD	6 * SIZE(%ecx)
	FLD	5 * SIZE(%ecx)
	FLD	4 * SIZE(%ecx)
	FLD	3 * SIZE(%ecx)
	FLD	2 * SIZE(%ecx)
	FLD	1 * SIZE(%ecx)
	FLD	0 * SIZE(%ecx)

	FST	0 * SIZE(%edx)
	FST	1 * SIZE(%edx)
	FST	2 * SIZE(%edx)
	FST	3 * SIZE(%edx)
	FST	4 * SIZE(%edx)
	FST	5 * SIZE(%edx)
	FST	6 * SIZE(%edx)
	FST	7 * SIZE(%edx)

	addl	$8 * SIZE, %ecx
	addl	$8 * SIZE, %edx
	decl	%eax
	jg	.L11
	ALIGN_2

.L20:
	movl	%ebx, %eax	# i = m
	andl	$7,   %eax
	jle	.L99
	ALIGN_2

.L21:
	FLD	(%ecx)
	FST	(%edx)
	addl	$SIZE, %ecx
	addl	$SIZE, %edx
	decl	%eax
	jg	.L21

.L99:
	xorl	%eax,%eax
	popl	%ebx
	popl	%esi
	popl	%edi
	ret
	ALIGN_3

.L100:
	movl	%ebx, %eax
	sarl	$3,   %eax
	jle	.L120
	ALIGN_2

.L111:
	FLD	(%ecx)
	addl	%esi, %ecx
	FLD	(%ecx)
	addl	%esi, %ecx
	FLD	(%ecx)
	addl	%esi, %ecx
	FLD	(%ecx)
	addl	%esi, %ecx
	FLD	(%ecx)
	addl	%esi, %ecx
	FLD	(%ecx)
	addl	%esi, %ecx
	FLD	(%ecx)
	addl	%esi, %ecx
	FLD	(%ecx)
	addl	%esi, %ecx

	fxch	%st(7)
	FST	(%edx)
	addl	%edi, %edx

	fxch	%st(5)
	FST	(%edx)
	addl	%edi, %edx

	fxch	%st(3)
	FST	(%edx)
	addl	%edi, %edx

	fxch	%st(1)
	FST	(%edx)
	addl	%edi, %edx

	FST	(%edx)
	addl	%edi, %edx

	FST	(%edx)
	addl	%edi, %edx

	FST	(%edx)
	addl	%edi, %edx

	FST	(%edx)
	addl	%edi, %edx

	decl	%eax
	jg	.L111

.L120:
	movl	%ebx, %eax
	andl	$7,   %eax
	jle	.L999
	ALIGN_2

.L121:
	FLD	(%ecx)
	FST	(%edx)
	addl	%esi, %ecx
	addl	%edi, %edx
	decl	%eax
	jg	.L121

.L999:
	xorl	%eax,%eax
	popl	%ebx
	popl	%esi
	popl	%edi
	ret

	EPILOGUE
