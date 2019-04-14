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
#define STACK_X		 8 + STACK + ARGS(%esp)
#define STACK_INCX	12 + STACK + ARGS(%esp)

#define	M	%ebx
#define X	%ecx
#define INCX	%edx
#define I	%eax

#ifdef USE_MIN
#define maxps	minps
#define maxss	minss
#endif

#include "l1param.h"

	PROLOGUE

	pushl	%ebp
	pushl	%edi
	pushl	%esi
	pushl	%ebx

	PROFCODE

	movl	STACK_M, M
	movl	STACK_X, X
	movl	STACK_INCX, INCX

#ifdef F_INTERFACE
	movl	(M), M
	movl	(INCX), INCX
#endif

	xorps	%xmm0, %xmm0
	leal	(, INCX, SIZE), INCX

	testl	M, M
	jle	.L999

#ifdef USE_ABS
#ifndef HAVE_SSE2
	subl	$8, %esp
	movl	$0x7fffffff, (%esp)
	movss	(%esp), %xmm3
	shufps	$0, %xmm3, %xmm3
	addl	$8, %esp
#else
	pcmpeqb	%xmm3, %xmm3
	psrld	$1, %xmm3
#endif
#endif

	movss	(X), %xmm0
	shufps	$0,  %xmm0, %xmm0
#ifdef USE_ABS
	andps	%xmm3, %xmm0
#endif
	movaps	%xmm0, %xmm1
	addl	INCX, X
	decl	M
	jle	.L999

	cmpl	$SIZE, INCX
	jne	.L40

	subl	$-32 * SIZE, X

	cmpl	$3, M
	jle	.L17

	testl	$SIZE, X
	je	.L05

	movss	-32 * SIZE(X), %xmm4
	addl	$SIZE, X
	shufps	$0, %xmm4, %xmm4
#ifdef USE_ABS
	andps	%xmm3, %xmm4
#endif
	maxps	%xmm4, %xmm0

	decl	M
	ALIGN_3

.L05:
	testl	$2 * SIZE, X
	je	.L06

	movsd	-32 * SIZE(X), %xmm4
	addl	$2 * SIZE, X
	unpcklps  %xmm4, %xmm4
#ifdef USE_ABS
	andps	%xmm3, %xmm4
#endif
	maxps	%xmm4, %xmm1

	subl	$2, M
	ALIGN_3

.L06:
	movl	M,  I
	sarl	$5, I
	jle	.L15

	movaps	-32 * SIZE(X), %xmm4
	movaps	-28 * SIZE(X), %xmm5
	movaps	-24 * SIZE(X), %xmm6
	movaps	-20 * SIZE(X), %xmm7

	decl	I
	jle	.L12
	ALIGN_4

.L11:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

#ifdef USE_ABS
	andps	%xmm3, %xmm4
#endif
	maxps	%xmm4, %xmm0
	movaps	-16 * SIZE(X), %xmm4

#ifdef USE_ABS
	andps	%xmm3, %xmm5
#endif
	maxps	%xmm5, %xmm1
	movaps	-12 * SIZE(X), %xmm5

#ifdef USE_ABS
	andps	%xmm3, %xmm6
#endif
	maxps	%xmm6, %xmm0
	movaps	 -8 * SIZE(X), %xmm6

#ifdef USE_ABS
	andps	%xmm3, %xmm7
#endif
	maxps	%xmm7, %xmm1
	movaps	 -4 * SIZE(X), %xmm7

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE + 64) - PREOFFSET(X)
#endif

#ifdef USE_ABS
	andps	%xmm3, %xmm4
#endif
	maxps	%xmm4, %xmm0
	movaps	  0 * SIZE(X), %xmm4

#ifdef USE_ABS
	andps	%xmm3, %xmm5
#endif
	maxps	%xmm5, %xmm1
	movaps	  4 * SIZE(X), %xmm5

#ifdef USE_ABS
	andps	%xmm3, %xmm6
#endif
	maxps	%xmm6, %xmm0
	movaps	  8 * SIZE(X), %xmm6

#ifdef USE_ABS
	andps	%xmm3, %xmm7
#endif
	maxps	%xmm7, %xmm1
	movaps	 12 * SIZE(X), %xmm7

	subl	$-32 * SIZE, X
	decl	I
	jg	.L11
	ALIGN_4

.L12:
#ifdef USE_ABS
	andps	%xmm3, %xmm4
#endif
	maxps	%xmm4, %xmm0
	movaps	-16 * SIZE(X), %xmm4

#ifdef USE_ABS
	andps	%xmm3, %xmm5
#endif
	maxps	%xmm5, %xmm1
	movaps	-12 * SIZE(X), %xmm5

#ifdef USE_ABS
	andps	%xmm3, %xmm6
#endif
	maxps	%xmm6, %xmm0
	movaps	 -8 * SIZE(X), %xmm6

#ifdef USE_ABS
	andps	%xmm3, %xmm7
#endif
	maxps	%xmm7, %xmm1
	movaps	 -4 * SIZE(X), %xmm7

#ifdef USE_ABS
	andps	%xmm3, %xmm4
#endif
	maxps	%xmm4, %xmm0

#ifdef USE_ABS
	andps	%xmm3, %xmm5
#endif
	maxps	%xmm5, %xmm1

#ifdef USE_ABS
	andps	%xmm3, %xmm6
#endif
	maxps	%xmm6, %xmm0

#ifdef USE_ABS
	andps	%xmm3, %xmm7
#endif
	maxps	%xmm7, %xmm1

	subl	$-32 * SIZE, X
	ALIGN_3


.L15:
	testl	$16, M
	je	.L16

	movaps	-32 * SIZE(X), %xmm4
#ifdef USE_ABS
	andps	%xmm3, %xmm4
#endif
	maxps	%xmm4, %xmm0

	movaps	-28 * SIZE(X), %xmm5
#ifdef USE_ABS
	andps	%xmm3, %xmm5
#endif
	maxps	%xmm5, %xmm1

	movaps	-24 * SIZE(X), %xmm6
#ifdef USE_ABS
	andps	%xmm3, %xmm6
#endif
	maxps	%xmm6, %xmm0

	movaps	-20 * SIZE(X), %xmm7
#ifdef USE_ABS
	andps	%xmm3, %xmm7
#endif
	maxps	%xmm7, %xmm1

	addl	$16 * SIZE, X
	ALIGN_3

.L16:
	testl	$8, M
	je	.L17

	movaps	-32 * SIZE(X), %xmm4
#ifdef USE_ABS
	andps	%xmm3, %xmm4
#endif
	maxps	%xmm4, %xmm0

	movaps	-28 * SIZE(X), %xmm5
#ifdef USE_ABS
	andps	%xmm3, %xmm5
#endif
	maxps	%xmm5, %xmm1
	addl	$8 * SIZE, X
	ALIGN_3

.L17:
	testl	$4, M
	je	.L18

	movaps	-32 * SIZE(X), %xmm4
#ifdef USE_ABS
	andps	%xmm3, %xmm4
#endif
	maxps	%xmm4, %xmm0
	addl	$4 * SIZE, X
	ALIGN_3

.L18:
	testl	$2, M
	je	.L19

	movsd	-32 * SIZE(X), %xmm4
	unpcklps %xmm4, %xmm4
#ifdef USE_ABS
	andps	%xmm3, %xmm4
#endif
	maxps	%xmm4, %xmm1
	addl	$2 * SIZE, X
	ALIGN_3

.L19:
	testl	$1, M
	je	.L998

	movss	-32 * SIZE(X), %xmm4
#ifdef USE_ABS
	andps	%xmm3, %xmm4
#endif
	maxss	%xmm4, %xmm0
	jmp	.L998
	ALIGN_3

.L40:
	movl	M,  I
	sarl	$3, I
	jle	.L45
	ALIGN_4

.L41:
	movss	(X), %xmm4
	addl	INCX, X
#ifdef USE_ABS
	andps	%xmm3, %xmm4
#endif
	maxss	%xmm4, %xmm0

	movss	(X), %xmm5
	addl	INCX, X
#ifdef USE_ABS
	andps	%xmm3, %xmm5
#endif
	maxss	%xmm5, %xmm1

	movss	(X), %xmm6
	addl	INCX, X
#ifdef USE_ABS
	andps	%xmm3, %xmm6
#endif
	maxss	%xmm6, %xmm0

	movss	(X), %xmm7
	addl	INCX, X
#ifdef USE_ABS
	andps	%xmm3, %xmm7
#endif
	maxss	%xmm7, %xmm1

	movss	(X), %xmm4
	addl	INCX, X
#ifdef USE_ABS
	andps	%xmm3, %xmm4
#endif
	maxss	%xmm4, %xmm0

	movss	(X), %xmm5
	addl	INCX, X
#ifdef USE_ABS
	andps	%xmm3, %xmm5
#endif
	maxss	%xmm5, %xmm1

	movss	(X), %xmm6
	addl	INCX, X
#ifdef USE_ABS
	andps	%xmm3, %xmm6
#endif
	maxss	%xmm6, %xmm0

	movss	(X), %xmm7
	addl	INCX, X
#ifdef USE_ABS
	andps	%xmm3, %xmm7
#endif
	maxss	%xmm7, %xmm1

	decl	I
	jg	.L41
	ALIGN_4

.L45:
	testl	$4, M
	je	.L46

	movss	(X), %xmm4
	addl	INCX, X
#ifdef USE_ABS
	andps	%xmm3, %xmm4
#endif
	maxss	%xmm4, %xmm0

	movss	(X), %xmm5
	addl	INCX, X
#ifdef USE_ABS
	andps	%xmm3, %xmm5
#endif
	maxss	%xmm5, %xmm1

	movss	(X), %xmm6
	addl	INCX, X
#ifdef USE_ABS
	andps	%xmm3, %xmm6
#endif
	maxss	%xmm6, %xmm0

	movss	(X), %xmm7
	addl	INCX, X
#ifdef USE_ABS
	andps	%xmm3, %xmm7
#endif
	maxss	%xmm7, %xmm1
	ALIGN_3

.L46:
	testl	$2, M
	je	.L47

	movss	(X), %xmm4
	addl	INCX, X
#ifdef USE_ABS
	andps	%xmm3, %xmm4
#endif
	maxss	%xmm4, %xmm0

	movss	(X), %xmm5
	addl	INCX, X
#ifdef USE_ABS
	andps	%xmm3, %xmm5
#endif
	maxss	%xmm5, %xmm1
	ALIGN_3

.L47:
	testl	$1, M
	je	.L998

	movss	(X), %xmm4
	addl	INCX, X
#ifdef USE_ABS
	andps	%xmm3, %xmm4
#endif
	maxss	%xmm4, %xmm0
	ALIGN_4

.L998:
	maxps	%xmm1, %xmm0
	movaps	%xmm0, %xmm1
	movhlps %xmm0, %xmm0
	maxps	%xmm1, %xmm0
	movaps	%xmm0, %xmm1
	shufps	$1, %xmm0, %xmm0
	maxss	%xmm1, %xmm0
	ALIGN_4

.L999:

	subl	$8, %esp
	movss	%xmm0, (%esp)
	flds	(%esp)
	addl	$8, %esp
	popl	%ebx
	popl	%esi
	popl	%edi
	popl	%ebp
	ret

	EPILOGUE
