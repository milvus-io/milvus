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
#define ARGS     0

#define STACK_M		 4 + STACK + ARGS(%esp)
#define STACK_X		 8 + STACK + ARGS(%esp)
#define STACK_INCX	12 + STACK + ARGS(%esp)

#define I	%eax
#define M	%ecx
#define X	%esi
#define INCX	%ebx

#include "l1param.h"

	PROLOGUE
	PROFCODE

	pushl	%esi
	pushl	%ebx

	movl	STACK_M, M
	movl	STACK_X, X
	movl	STACK_INCX, INCX

	xorps	%xmm0, %xmm0
	xorps	%xmm1, %xmm1

	testl	M, M
	jle	.L999
	testl	INCX, INCX
	jle	.L999

#ifdef HAVE_SSE2
	pcmpeqb	%xmm3, %xmm3
	psrld	$1, %xmm3
#else
	movl	$0x7fffffff, STACK_M
	movss	STACK_M, %xmm3
	shufps	$0, %xmm3, %xmm3
#endif

	sall	$ZBASE_SHIFT, INCX

	cmpl	$2 * SIZE, INCX
	jne	.L100

	subl	$-32 * SIZE, X
	addl	M, M

	cmpl	$3, M
	jle	.L18

	testl	$4, X
	je	.L05
	movss	-32 * SIZE(X), %xmm0
	andps	%xmm3, %xmm0
	addl	$SIZE, X
	decl	M
	jle	.L999
	ALIGN_3

.L05:
	testl	$8, X
	je	.L10

	movsd	-32 * SIZE(X), %xmm1
	andps	%xmm3, %xmm1
	addl	$2 * SIZE, X
	subl	$2, M
	jle	.L999
	ALIGN_3

.L10:
	movl	M,  I
	sarl	$5, I
	jle	.L14

	movaps	-32 * SIZE(X), %xmm4
	movaps	-28 * SIZE(X), %xmm5
	movaps	-24 * SIZE(X), %xmm6
	movaps	-20 * SIZE(X), %xmm7

	decl	I
	jle	.L12
	ALIGN_3

.L11:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	andps	%xmm3, %xmm4
	addps	%xmm4, %xmm0
	movaps	-16 * SIZE(X), %xmm4

	andps	%xmm3, %xmm5
	addps	%xmm5, %xmm1
	movaps	-12 * SIZE(X), %xmm5

	andps	%xmm3, %xmm6
	addps	%xmm6, %xmm0
	movaps	 -8 * SIZE(X), %xmm6

	andps	%xmm3, %xmm7
	addps	%xmm7, %xmm1
	movaps	 -4 * SIZE(X), %xmm7

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	andps	%xmm3, %xmm4
	addps	%xmm4, %xmm0
	movaps	  0 * SIZE(X), %xmm4

	andps	%xmm3, %xmm5
	addps	%xmm5, %xmm1
	movaps	  4 * SIZE(X), %xmm5

	andps	%xmm3, %xmm6
	addps	%xmm6, %xmm0
	movaps	  8 * SIZE(X), %xmm6

	andps	%xmm3, %xmm7
	addps	%xmm7, %xmm1
	movaps	 12 * SIZE(X), %xmm7

	subl	$-32 * SIZE, X
	decl	I
	jg	.L11
	ALIGN_3

.L12:
	andps	%xmm3, %xmm4
	addps	%xmm4, %xmm0
	movaps	-16 * SIZE(X), %xmm4

	andps	%xmm3, %xmm5
	addps	%xmm5, %xmm1
	movaps	-12 * SIZE(X), %xmm5

	andps	%xmm3, %xmm6
	addps	%xmm6, %xmm0
	movaps	 -8 * SIZE(X), %xmm6

	andps	%xmm3, %xmm7
	addps	%xmm7, %xmm1
	movaps	 -4 * SIZE(X), %xmm7

	andps	%xmm3, %xmm4
	addps	%xmm4, %xmm0
	andps	%xmm3, %xmm5
	addps	%xmm5, %xmm1

	andps	%xmm3, %xmm6
	addps	%xmm6, %xmm0
	andps	%xmm3, %xmm7
	addps	%xmm7, %xmm1

	addl	$32 * SIZE, X
	ALIGN_3

.L14:
	testl	$16, M
	je	.L16

	movaps	-32 * SIZE(X), %xmm4
	andps	%xmm3, %xmm4
	addps	%xmm4, %xmm0

	movaps	-28 * SIZE(X), %xmm5
	andps	%xmm3, %xmm5
	addps	%xmm5, %xmm1

	movaps	-24 * SIZE(X), %xmm6
	andps	%xmm3, %xmm6
	addps	%xmm6, %xmm0

	movaps	-20 * SIZE(X), %xmm7
	andps	%xmm3, %xmm7
	addps	%xmm7, %xmm1

	addl	$16 * SIZE, X
	ALIGN_3

.L16:
	testl	$8, M
	je	.L17

	movaps	-32 * SIZE(X), %xmm4
	andps	%xmm3, %xmm4
	addps	%xmm4, %xmm0

	movaps	-28 * SIZE(X), %xmm5
	andps	%xmm3, %xmm5
	addps	%xmm5, %xmm1

	addl	$8 * SIZE, X
	ALIGN_3

.L17:
	testl	$4, M
	je	.L18

	movaps	-32 * SIZE(X), %xmm4
	andps	%xmm3, %xmm4
	addps	%xmm4, %xmm0
	addl	$4 * SIZE, X
	ALIGN_3

.L18:
	testl	$2, M
	je	.L19

#ifdef movsd
	xorps	%xmm4, %xmm4
#endif
	movsd	-32 * SIZE(X), %xmm4
	andps	%xmm3, %xmm4
	addps	%xmm4, %xmm1
	addl	$2 * SIZE, X
	ALIGN_3

.L19:
	testl	$1, M
	je	.L999

	movss	-32 * SIZE(X), %xmm4
	andps	%xmm3, %xmm4
	addps	%xmm4, %xmm0
	jmp	.L999
	ALIGN_4

.L100:
	movl	M,  I
	sarl	$2, I
	jle	.L105
	ALIGN_4

.L101:
	movsd	(X), %xmm4
	addl	INCX, X
	movhps	(X), %xmm4
	addl	INCX, X

	andps	%xmm3, %xmm4
	addps	%xmm4, %xmm0

	movsd	(X), %xmm5
	addl	INCX, X
	movhps	(X), %xmm5
	addl	INCX, X

	andps	%xmm3, %xmm5
	addps	%xmm5, %xmm1

	decl	I
	jg	.L101
	ALIGN_4

.L105:
#ifdef movsd
	xorps	%xmm4, %xmm4
#endif
	andl	$3,  M
	jle	.L999
	ALIGN_4

.L106:
	movsd	(X),  %xmm4
	andps	%xmm3, %xmm4
	addps	%xmm4, %xmm0
	addl	INCX, X
	decl	M
	jg	.L106
	ALIGN_4

.L999:
	addps	%xmm1, %xmm0

#ifndef HAVE_SSE3
	movhlps	%xmm0, %xmm1
	addps	%xmm1, %xmm0

	movaps	%xmm0, %xmm1
	shufps  $1, %xmm0, %xmm0
	addss	 %xmm1, %xmm0
#else
	haddps	%xmm0, %xmm0
	haddps	%xmm0, %xmm0
#endif

	movss	%xmm0, STACK_M
	flds	STACK_M

	popl	%ebx
	popl	%esi
	ret

	EPILOGUE
