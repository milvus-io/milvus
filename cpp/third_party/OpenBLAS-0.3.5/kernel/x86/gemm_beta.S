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

#define M	 4 + STACK + ARGS(%esp)
#define N	 8 + STACK + ARGS(%esp)
#ifdef DOUBLE
#define BETA	16 + STACK + ARGS(%esp)
#define C	40 + STACK + ARGS(%esp)
#define LDC	44 + STACK + ARGS(%esp)
#else
#define BETA	16 + STACK + ARGS(%esp)
#define C	36 + STACK + ARGS(%esp)
#define LDC	40 + STACK + ARGS(%esp)
#endif

	PROLOGUE

	pushl	%ebp
	pushl	%edi
	pushl	%esi
	pushl	%ebx

	PROFCODE

	movl	M, %esi			# m
	movl	N, %ecx			# n
	FLD	BETA			# beta

	movl	C,   %edi		# C
	movl	LDC, %ebp		# ldc

	testl	%esi, %esi		# if n <= 0 goto End
	jle	.L999
	testl	%ecx, %ecx		# if m <= 0 goto End
	jle	.L999

	ftst
	fnstsw	%ax
	andb	$68, %ah
	je	.L201
	ALIGN_4

.L101:
	movl	 %edi, %eax			# c_offset = c
	leal	(%edi, %ebp, SIZE), %edi	# c += ldc
	movl	%esi, %edx
	sarl	$3,   %edx
	jle	.L103
	ALIGN_4

.L102:
#ifdef HAS_PREFETCH
#ifndef ATHLON
	prefetchnta	12 * SIZE(%eax)
	prefetchnta	16 * SIZE(%eax)
#else
	prefetchw	32 * SIZE(%eax)
#endif
#endif

	FSTU	0 * SIZE(%eax)
	FSTU	1 * SIZE(%eax)
	FSTU	2 * SIZE(%eax)
	FSTU	3 * SIZE(%eax)
	FSTU	4 * SIZE(%eax)
	FSTU	5 * SIZE(%eax)
	FSTU	6 * SIZE(%eax)
	FSTU	7 * SIZE(%eax)
	addl	$8 * SIZE, %eax
	decl	%edx
	jg	.L102
	ALIGN_4

.L103:
	movl	%esi, %edx
	andl	$7,   %edx
	jle	.L105
	ALIGN_4

.L104:
	FSTU	0 * SIZE(%eax)
	addl	$SIZE, %eax
	decl	%edx
	jg	.L104
	ALIGN_4

.L105:
	decl	%ecx
	jg	.L101
	jmp	.L999
	ALIGN_3


.L201:
	movl	 %edi, %eax			# c_offset = c
	leal	(%edi, %ebp, SIZE), %edi	# c += ldc
	movl	%esi, %edx
	sarl	$3,   %edx
	jle	.L203
	ALIGN_4

.L202:
#ifdef HAS_PREFETCH
#ifndef ATHLON
	prefetchnta	16 * SIZE(%eax)
	prefetchnta	20 * SIZE(%eax)
#else
	prefetchw	32 * SIZE(%eax)
#endif
#endif

	FLD	0 * SIZE(%eax)
	fmul	%st(1),%st
	FST	0 * SIZE(%eax)

	FLD	1 * SIZE(%eax)
	fmul	%st(1),%st
	FST	1 * SIZE(%eax)

	FLD	2 * SIZE(%eax)
	fmul	%st(1),%st
	FST	2 * SIZE(%eax)

	FLD	3 * SIZE(%eax)
	fmul	%st(1),%st
	FST	3 * SIZE(%eax)

	FLD	4 * SIZE(%eax)
	fmul	%st(1),%st
	FST	4 * SIZE(%eax)

	FLD	5 * SIZE(%eax)
	fmul	%st(1),%st
	FST	5 * SIZE(%eax)

	FLD	6 * SIZE(%eax)
	fmul	%st(1),%st
	FST	6 * SIZE(%eax)

	FLD	7 * SIZE(%eax)
	fmul	%st(1),%st
	FST	7 * SIZE(%eax)

	addl	$8 * SIZE, %eax
	decl	%edx
	jg	.L202
	ALIGN_4

.L203:
	movl	%esi, %edx
	andl	$7,   %edx
	jle	.L205
	ALIGN_4

.L204:
	FLD	0 * SIZE(%eax)
	fmul	%st(1), %st
	FST	0 * SIZE(%eax)
	addl	$SIZE, %eax
	decl	%edx
	jg	.L204
	ALIGN_4

.L205:
	decl	%ecx
	jg	.L201
	ALIGN_3

.L999:
#ifndef C_SUN
	ffreep	%st(0)
#else
	.byte	0xdf
	.byte	0xc0
#endif
	xorl	%eax,%eax
	popl	%ebx
	popl	%esi
	popl	%edi
	popl	%ebp
	ret

	EPILOGUE
