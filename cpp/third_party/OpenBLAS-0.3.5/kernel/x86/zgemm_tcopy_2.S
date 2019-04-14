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
#define ARGS	 8

#define J	 0 + STACK(%esp)
#define BOFFSET2 4 + STACK(%esp)

#define STACK_M	 4 + STACK + ARGS(%esp)
#define STACK_N	 8 + STACK + ARGS(%esp)
#define STACK_A	12 + STACK + ARGS(%esp)
#define STACK_LDA	16 + STACK + ARGS(%esp)
#define STACK_B	20 + STACK + ARGS(%esp)

	PROLOGUE

	subl	$ARGS, %esp
	pushl	%ebp
	pushl	%edi
	pushl	%esi
	pushl	%ebx

	PROFCODE

#define A	%ebp
#define A1	%edx
#define LDA	%ecx
#define B	%edi
#define I	%ebx
#define B1	%eax
#define M4	%esi

	EMMS

	movl	STACK_A, A
	movl	STACK_B, B

	movl	STACK_M,   %ebx
	movl	STACK_N,   %eax
	movl	STACK_LDA, LDA
	sall	$ZBASE_SHIFT, LDA

	andl	$-2,  %eax
	addl	%eax, %eax
	imull	%ebx, %eax		# m * ( n & ~1)
	leal	(B, %eax, SIZE), %eax	# boffset2 = b + m * (n & ~1)
	movl	%eax, BOFFSET2

	movl	STACK_M, M4
	sall	$ZBASE_SHIFT + 1, M4

	testl	%ebx,  %ebx		# if !(m & 1) goto L28
	movl	%ebx, J
	jle	.L999
	ALIGN_4

.L39:
	movl	A, A1
	addl	LDA, A
	movl	B, B1
	addl	$4 * SIZE, B

	movl	STACK_N,   I
	sarl	$1, I
	jle	.L32
	ALIGN_4

.L36:
#ifdef HAVE_MMX
	MMXLOAD		0 * SIZE(A1), %mm0
	MMXLOAD		1 * SIZE(A1), %mm1
	MMXLOAD		2 * SIZE(A1), %mm2
	MMXLOAD		3 * SIZE(A1), %mm3

	MMXSTORE	%mm0, 0 * SIZE(B1)
	MMXSTORE	%mm1, 1 * SIZE(B1)
	MMXSTORE	%mm2, 2 * SIZE(B1)
	MMXSTORE	%mm3, 3 * SIZE(B1)
#else
	FLD	3 * SIZE(A1)
	FLD	2 * SIZE(A1)
	FLD	1 * SIZE(A1)
	FLD	0 * SIZE(A1)

	FST	0 * SIZE(B1)
	FST	1 * SIZE(B1)
	FST	2 * SIZE(B1)
	FST	3 * SIZE(B1)
#endif
	addl	$4 * SIZE, A1
	addl	M4, B1
	decl	I
	jne	.L36
	ALIGN_4

.L32:
	movl	STACK_N,   I
	andl	$1, I
	jle	.L99
	ALIGN_4

	movl	BOFFSET2, B1

#ifdef HAVE_MMX
	MMXLOAD		0 * SIZE(A1), %mm0
	MMXLOAD		1 * SIZE(A1), %mm1

	MMXSTORE	%mm0, 0 * SIZE(B1)
	MMXSTORE	%mm1, 1 * SIZE(B1)
#else
	FLD	1 * SIZE(A1)
	FLD	0 * SIZE(A1)

	FST	0 * SIZE(B1)
	FST	1 * SIZE(B1)
#endif
	addl	$2 * SIZE, BOFFSET2
	ALIGN_4

.L99:
	decl	J
	jne	.L39
	ALIGN_4

.L999:
	EMMS

	popl	%ebx
	popl	%esi
	popl	%edi
	popl	%ebp
	addl	$ARGS,%esp
	ret

	EPILOGUE
