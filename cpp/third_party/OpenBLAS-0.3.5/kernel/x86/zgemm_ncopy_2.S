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

#define STACK_M	 4 + STACK + ARGS(%esp)
#define STACK_N	 8 + STACK + ARGS(%esp)
#define STACK_A	12 + STACK + ARGS(%esp)
#define STACK_LDA	16 + STACK + ARGS(%esp)
#define STACK_B	20 + STACK + ARGS(%esp)

#define I	%eax
#define J	%ecx
#define LDA	%edx
#define A	%edi
#define A1	%ebx
#define A2	%ebp
#define B	%esi

	PROLOGUE

	subl	$ARGS, %esp
	pushl	%ebp
	pushl	%edi
	pushl	%esi
	pushl	%ebx

	PROFCODE

	EMMS

	movl	STACK_A,   A
	movl	STACK_LDA, LDA
	movl	STACK_B, B
	sall	$ZBASE_SHIFT, LDA

	movl	STACK_N,  J
	sarl	$1, J
	je	.L20
	ALIGN_3

.L21:
	movl	A, A1
	leal    (A1, LDA), A2
	leal	(A,  LDA, 2), A

	movl	STACK_M,   I
	sarl	$1,  I
	je	.L24
	ALIGN_3

.L25:
#ifdef HAVE_MMX
	MMXLOAD		0 * SIZE(A1), %mm0
	MMXLOAD		1 * SIZE(A1), %mm1
	MMXLOAD		0 * SIZE(A2), %mm2
	MMXLOAD		1 * SIZE(A2), %mm3

	MMXLOAD		2 * SIZE(A1), %mm4
	MMXLOAD		3 * SIZE(A1), %mm5
	MMXLOAD		2 * SIZE(A2), %mm6
	MMXLOAD		3 * SIZE(A2), %mm7

	MMXSTORE	%mm0, 0 * SIZE(B)
	MMXSTORE	%mm1, 1 * SIZE(B)
	MMXSTORE	%mm2, 2 * SIZE(B)
	MMXSTORE	%mm3, 3 * SIZE(B)

	MMXSTORE	%mm4, 4 * SIZE(B)
	MMXSTORE	%mm5, 5 * SIZE(B)
	MMXSTORE	%mm6, 6 * SIZE(B)
	MMXSTORE	%mm7, 7 * SIZE(B)
#else
	FLD	3 * SIZE(A2)
	FLD	2 * SIZE(A2)
	FLD	3 * SIZE(A1)
	FLD	2 * SIZE(A1)
	FLD	1 * SIZE(A2)
	FLD	0 * SIZE(A2)
	FLD	1 * SIZE(A1)
	FLD	0 * SIZE(A1)

	FST	0 * SIZE(B)
	FST	1 * SIZE(B)
	FST	2 * SIZE(B)
	FST	3 * SIZE(B)
	FST	4 * SIZE(B)
	FST	5 * SIZE(B)
	FST	6 * SIZE(B)
	FST	7 * SIZE(B)
#endif
	addl	$4 * SIZE, A1
	addl	$4 * SIZE, A2
	addl	$8 * SIZE, B
	decl	I
	jne	.L25
	ALIGN_3

.L24:
	movl	STACK_M,  I
	andl	$1, I
	jle	.L30
	ALIGN_3

.L31:
#ifdef HAVE_MMX
	MMXLOAD		0 * SIZE(A1), %mm0
	MMXLOAD		1 * SIZE(A1), %mm1
	MMXLOAD		0 * SIZE(A2), %mm2
	MMXLOAD		1 * SIZE(A2), %mm3
	MMXSTORE	%mm0, 0 * SIZE(B)
	MMXSTORE	%mm1, 1 * SIZE(B)
	MMXSTORE	%mm2, 2 * SIZE(B)
	MMXSTORE	%mm3, 3 * SIZE(B)
#else
	FLD	1 * SIZE(A2)
	FLD	0 * SIZE(A2)
	FLD	1 * SIZE(A1)
	FLD	0 * SIZE(A1)
	FST	0 * SIZE(B)
	FST	1 * SIZE(B)
	FST	2 * SIZE(B)
	FST	3 * SIZE(B)
#endif
	addl	$2 * SIZE, A1
	addl	$2 * SIZE, A2
	addl	$4 * SIZE, B
	decl	I
	jne	.L31
	ALIGN_3

.L30:
	decl	J
	jne	.L21
	ALIGN_3

.L20:
	movl	A, A1
	movl	STACK_N, J
	andl	$1, J
	jle	.L38
	ALIGN_3

.L39:
	movl	STACK_M,  I
	sarl	$2, I
	je	.L42
	ALIGN_3

.L43:
#ifdef HAVE_MMX
	MMXLOAD		0 * SIZE(A1), %mm0
	MMXLOAD		1 * SIZE(A1), %mm1
	MMXLOAD		2 * SIZE(A1), %mm2
	MMXLOAD		3 * SIZE(A1), %mm3
	MMXLOAD		4 * SIZE(A1), %mm4
	MMXLOAD		5 * SIZE(A1), %mm5
	MMXLOAD		6 * SIZE(A1), %mm6
	MMXLOAD		7 * SIZE(A1), %mm7

	MMXSTORE	%mm0,  0 * SIZE(B)
	MMXSTORE	%mm1,  1 * SIZE(B)
	MMXSTORE	%mm2,  2 * SIZE(B)
	MMXSTORE	%mm3,  3 * SIZE(B)
	MMXSTORE	%mm4,  4 * SIZE(B)
	MMXSTORE	%mm5,  5 * SIZE(B)
	MMXSTORE	%mm6,  6 * SIZE(B)
	MMXSTORE	%mm7,  7 * SIZE(B)
#else
	FLD	 7 * SIZE(A1)
	FLD	 6 * SIZE(A1)
	FLD	 5 * SIZE(A1)
	FLD	 4 * SIZE(A1)
	FLD	 3 * SIZE(A1)
	FLD	 2 * SIZE(A1)
	FLD	 1 * SIZE(A1)
	FLD	 0 * SIZE(A1)

	FST	 0 * SIZE(B)
	FST	 1 * SIZE(B)
	FST	 2 * SIZE(B)
	FST	 3 * SIZE(B)
	FST	 4 * SIZE(B)
	FST	 5 * SIZE(B)
	FST	 6 * SIZE(B)
	FST	 7 * SIZE(B)
#endif

	addl	$8 * SIZE, A1
	addl	$8 * SIZE, B
	decl	I
	jne	.L43
	ALIGN_3

.L42:
	movl	STACK_M,  I
	andl	$3, I
	jle	.L38
	ALIGN_3

.L49:
#ifdef HAVE_MMX
	MMXLOAD		0 * SIZE(A1), %mm0
	MMXLOAD		1 * SIZE(A1), %mm1
	MMXSTORE	%mm0,  0 * SIZE(B)
	MMXSTORE	%mm1,  1 * SIZE(B)
#else
	FLD	1 * SIZE(A1)
	FLD	0 * SIZE(A1)
	FST	0 * SIZE(B)
	FST	1 * SIZE(B)
#endif
	addl	$2 * SIZE, A1
	addl	$2 * SIZE, B
	decl	I
	jne	.L49
	ALIGN_3

.L38:
	EMMS

	popl	%ebx
	popl	%esi
	popl	%edi
	popl	%ebp
	addl	$ARGS, %esp
	ret

	EPILOGUE
