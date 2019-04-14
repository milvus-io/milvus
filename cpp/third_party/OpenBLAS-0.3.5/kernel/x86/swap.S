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
#define ARGS     0

#define N	 4 + STACK + ARGS(%esp)
#ifdef XDOUBLE
#define X	32 + STACK + ARGS(%esp)
#define INCX	36 + STACK + ARGS(%esp)
#define Y	40 + STACK + ARGS(%esp)
#define INCY	44 + STACK + ARGS(%esp)
#elif defined(DOUBLE)
#define X	24 + STACK + ARGS(%esp)
#define INCX	28 + STACK + ARGS(%esp)
#define Y	32 + STACK + ARGS(%esp)
#define INCY	36 + STACK + ARGS(%esp)
#else
#define X	20 + STACK + ARGS(%esp)
#define INCX	24 + STACK + ARGS(%esp)
#define Y	28 + STACK + ARGS(%esp)
#define INCY	32 + STACK + ARGS(%esp)
#endif

	PROLOGUE

	pushl	%ebp
	pushl	%edi
	pushl	%esi
	pushl	%ebx

	PROFCODE

#if defined(F_INTERFACE_GFORT) || defined(F_INTERFACE_G95)
	EMMS
#endif

	movl	N,    %edx
	movl	X,    %esi
	movl	Y,    %edi
	movl	INCX, %ebx
	movl	INCY, %ecx

	sall	$BASE_SHIFT, %ebx
	sall	$BASE_SHIFT, %ecx

	cmpl	$SIZE, %ebx
	jne	.L14
	cmpl	$SIZE, %ecx
	jne	.L14

	movl	%edx, %eax
	sarl	$2,   %eax
	jle	.L15
	ALIGN_3

.L16:
	FLD	3 * SIZE(%esi)
	FLD	2 * SIZE(%esi)
	FLD	1 * SIZE(%esi)
	FLD	0 * SIZE(%esi)
	FLD	3 * SIZE(%edi)
	FLD	2 * SIZE(%edi)
	FLD	1 * SIZE(%edi)
	FLD	0 * SIZE(%edi)

	FST	0 * SIZE(%esi)
	FST	1 * SIZE(%esi)
	FST	2 * SIZE(%esi)
	FST	3 * SIZE(%esi)
	FST	0 * SIZE(%edi)
	FST	1 * SIZE(%edi)
	FST	2 * SIZE(%edi)
	FST	3 * SIZE(%edi)

	addl	$4 * SIZE, %esi
	addl	$4 * SIZE, %edi
	decl	%eax
	jg	.L16
	ALIGN_3

.L15:
	movl	%edx, %eax
	andl	$3,   %eax
	jle	.L27
	ALIGN_3

.L22:
	FLD	(%esi)
	FLD	(%edi)
	FST	(%esi)
	FST	(%edi)
	addl	$SIZE, %esi
	addl	$SIZE, %edi
	decl	%eax
	jg	.L22
	jmp	.L27
	ALIGN_3

/* INCX != 1 or INCY != 1 */

.L14:
	cmpl	$0, %ebx
	jne	.L141
	cmpl	$0, %ecx
	jne	.L141
/* INCX == 0 and INCY == 0 */	
	jmp	.L27

.L141:	
	movl	%edx, %eax
	sarl	$2,   %eax
	jle	.L28
	ALIGN_2

.L29:
	FLD	(%esi)
	addl	%ebx, %esi
	FLD	(%esi)
	addl	%ebx, %esi
	FLD	(%esi)
	addl	%ebx, %esi
	FLD	(%esi)

	FLD	(%edi)
	addl	%ecx, %edi
	FLD	(%edi)
	addl	%ecx, %edi
	FLD	(%edi)
	addl	%ecx, %edi
	FLD	(%edi)

	FST	(%esi)
	subl	%ebx, %esi
	FST	(%esi)
	subl	%ebx, %esi
	FST	(%esi)
	subl	%ebx, %esi
	FST	(%esi)
	leal	(%esi, %ebx, 4), %esi

	FST	(%edi)
	subl	%ecx, %edi
	FST	(%edi)
	subl	%ecx, %edi
	FST	(%edi)
	subl	%ecx, %edi
	FST	(%edi)
	leal	(%edi, %ecx, 4), %edi

	decl	%eax
	jg	.L29
	ALIGN_3

.L28:
	movl	%edx, %eax
	andl	$3,   %eax
	jle	.L27
	ALIGN_3

.L35:
	FLD	(%esi)
	FLD	(%edi)
	FST	(%esi)
	addl	%ebx, %esi
	FST	(%edi)
	addl	%ecx, %edi
	decl	%eax
	jg	.L35
	ALIGN_3

.L27:
	xorl	%eax,%eax
	popl	%ebx
	popl	%esi
	popl	%edi
	popl	%ebp
	ret

	EPILOGUE
