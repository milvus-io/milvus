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

	pcmpeqb	%xmm3, %xmm3
	psrlq	$1,    %xmm3

	sall	$BASE_SHIFT, INCX

	subl	$-16 * SIZE, X

	cmpl	$SIZE, INCX
	jne	.L40

	testl	$SIZE, X
	je	.L05

	movsd	-16 * SIZE(X), %xmm0
	addl	$SIZE, X

	andps	%xmm3, %xmm0
	subl	$1, M
	jle	.L999
	ALIGN_3

.L05:
	movl	M,  I
	sarl	$4, I
	jle	.L20

	movaps	-16 * SIZE(X), %xmm4
	movaps	-14 * SIZE(X), %xmm5
	movaps	-12 * SIZE(X), %xmm6
	movaps	-10 * SIZE(X), %xmm7

	decl	I
	jle	.L11
	ALIGN_4

.L10:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	andps	%xmm3, %xmm4
	addpd	%xmm4, %xmm0
	movaps	 -8 * SIZE(X), %xmm4

	andps	%xmm3, %xmm5
	addpd	%xmm5, %xmm1
	movaps	 -6 * SIZE(X), %xmm5

	andps	%xmm3, %xmm6
	addpd	%xmm6, %xmm0
	movaps	 -4 * SIZE(X), %xmm6

	andps	%xmm3, %xmm7
	addpd	%xmm7, %xmm1
	movaps	 -2 * SIZE(X), %xmm7

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	andps	%xmm3, %xmm4
	addpd	%xmm4, %xmm0
	movaps	  0 * SIZE(X), %xmm4

	andps	%xmm3, %xmm5
	addpd	%xmm5, %xmm1
	movaps	  2 * SIZE(X), %xmm5

	andps	%xmm3, %xmm6
	addpd	%xmm6, %xmm0
	movaps	  4 * SIZE(X), %xmm6

	andps	%xmm3, %xmm7
	addpd	%xmm7, %xmm1
	movaps	  6 * SIZE(X), %xmm7

	subl	$-16 * SIZE, X
	decl	I
	jg	.L10
	ALIGN_4

.L11:
	andps	%xmm3, %xmm4
	addpd	%xmm4, %xmm0
	movaps	 -8 * SIZE(X), %xmm4

	andps	%xmm3, %xmm5
	addpd	%xmm5, %xmm1
	movaps	 -6 * SIZE(X), %xmm5

	andps	%xmm3, %xmm6
	addpd	%xmm6, %xmm0
	movaps	 -4 * SIZE(X), %xmm6

	andps	%xmm3, %xmm7
	addpd	%xmm7, %xmm1
	movaps	 -2 * SIZE(X), %xmm7

	andps	%xmm3, %xmm4
	addpd	%xmm4, %xmm0
	andps	%xmm3, %xmm5
	addpd	%xmm5, %xmm1
	andps	%xmm3, %xmm6
	addpd	%xmm6, %xmm0
	andps	%xmm3, %xmm7
	addpd	%xmm7, %xmm1

	subl	$-16 * SIZE, X
	ALIGN_3

.L20:
	andl	$15,  M
	jle	.L999

	testl	$8, M
	je	.L21

	movaps	-16 * SIZE(X), %xmm4
	movaps	-14 * SIZE(X), %xmm5
	movaps	-12 * SIZE(X), %xmm6
	movaps	-10 * SIZE(X), %xmm7

	andps	%xmm3, %xmm4
	addpd	%xmm4, %xmm0
	andps	%xmm3, %xmm5
	addpd	%xmm5, %xmm1
	andps	%xmm3, %xmm6
	addpd	%xmm6, %xmm0
	andps	%xmm3, %xmm7
	addpd	%xmm7, %xmm1
	addl	$8 * SIZE, X
	ALIGN_3

.L21:
	testl	$4, M
	je	.L22

	movaps	-16 * SIZE(X), %xmm4
	movaps	-14 * SIZE(X), %xmm5

	andps	%xmm3, %xmm4
	addpd	%xmm4, %xmm0
	andps	%xmm3, %xmm5
	addpd	%xmm5, %xmm1

	addl	$4 * SIZE, X
	ALIGN_3

.L22:
	testl	$2, M
	je	.L23

	movaps	-16 * SIZE(X), %xmm4
	andps	%xmm3, %xmm4
	addpd	%xmm4, %xmm0
	addl	$2 * SIZE, X

.L23:
	testl	$1, M
	je	.L999

#ifdef movsd
	xorps	%xmm4, %xmm4
#endif
	movsd	-16 * SIZE(X), %xmm4
	andps	%xmm3, %xmm4
	addsd	%xmm4, %xmm1
	jmp	.L999
	ALIGN_3

.L40:
	movl	M,  I
	sarl	$3, I
	jle	.L60
	ALIGN_4

.L50:
	movsd	-16 * SIZE(X), %xmm4
	addl	INCX, X
	movhps	-16 * SIZE(X), %xmm4
	addl	INCX, X
	andps	%xmm3, %xmm4
	addpd	%xmm4, %xmm0

	movsd	-16 * SIZE(X), %xmm5
	addl	INCX, X
	movhps	-16 * SIZE(X), %xmm5
	addl	INCX, X
	andps	%xmm3, %xmm5
	addpd	%xmm5, %xmm1

	movsd	-16 * SIZE(X), %xmm6
	addl	INCX, X
	movhps	-16 * SIZE(X), %xmm6
	addl	INCX, X
	andps	%xmm3, %xmm6
	addpd	%xmm6, %xmm0

	movsd	-16 * SIZE(X), %xmm7
	addl	INCX, X
	movhps	-16 * SIZE(X), %xmm7
	addl	INCX, X
	andps	%xmm3, %xmm7
	addpd	%xmm7, %xmm1

	decl	I
	jg	.L50
	ALIGN_4

.L60:
#ifdef movsd
	xorps	%xmm4, %xmm4
#endif
	andl	$7,  M
	jle	.L999
	ALIGN_4

.L61:
	movsd	-16 * SIZE(X), %xmm4
	andps	%xmm3, %xmm4
	addsd	%xmm4, %xmm0
	addl	INCX, X
	decl	M
	jg	.L61
	ALIGN_4

.L999:
	addpd	%xmm1, %xmm0

#ifndef HAVE_SSE3
	movaps	%xmm0,  %xmm1
	unpckhpd %xmm0, %xmm0
	addsd	 %xmm1, %xmm0
#else
	haddpd	%xmm0, %xmm0
#endif

	movsd	%xmm0, STACK_M
	fldl	STACK_M
	popl	%ebx
	popl	%esi
	ret

	EPILOGUE

