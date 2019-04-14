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

#define M	ARG1	/* rdi */
#define X	ARG2	/* rsi */
#define INCX	ARG3	/* rdx */

#define I	%rax

#include "l1param.h"

	PROLOGUE
	PROFCODE

	SAVEREGISTERS

	pxor	%xmm0, %xmm0
	testq	M, M
	jle	.L999
	testq	INCX, INCX
	jle	.L999

	pxor	%xmm1, %xmm1
	pxor	%xmm2, %xmm2
	pxor	%xmm3, %xmm3

	pcmpeqb	%xmm15, %xmm15
	psrld	$1, %xmm15

	salq	$ZBASE_SHIFT, INCX

	cmpq	$2 * SIZE, INCX
	jne	.L100

	subq	$-32 * SIZE, X
	addq	M, M

	cmpq	$3, M
	jle	.L18

	testq	$4, X
	je	.L05
	movss	-32 * SIZE(X), %xmm0
	andps	%xmm15, %xmm0
	addq	$SIZE, X
	decq	M
	jle	.L998
	ALIGN_3

.L05:
	testq	$8, X
	je	.L10

#ifdef movsd
	xorps	%xmm1, %xmm1
#endif
	movsd	-32 * SIZE(X), %xmm1
	andps	%xmm15, %xmm1
	addq	$2 * SIZE, X
	subq	$2, M
	jle	.L998
	ALIGN_3

.L10:
	movq	M,  I
	sarq	$5, I
	jle	.L14

	movaps	-32 * SIZE(X), %xmm4
	movaps	-28 * SIZE(X), %xmm5
	movaps	-24 * SIZE(X), %xmm6
	movaps	-20 * SIZE(X), %xmm7

	movaps	-16 * SIZE(X), %xmm8
	movaps	-12 * SIZE(X), %xmm9
	movaps	 -8 * SIZE(X), %xmm10
	movaps	 -4 * SIZE(X), %xmm11
	decq	I
	jle	.L12
	ALIGN_3

.L11:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	andps	%xmm15, %xmm4
	addps	%xmm4,  %xmm0
	movaps	  0 * SIZE(X), %xmm4

	andps	%xmm15, %xmm5
	addps	%xmm5,  %xmm1
	movaps	  4 * SIZE(X), %xmm5

	andps	%xmm15, %xmm6
	addps	%xmm6,  %xmm2
	movaps	  8 * SIZE(X), %xmm6

	andps	%xmm15, %xmm7
	addps	%xmm7,  %xmm3
	movaps	 12 * SIZE(X), %xmm7

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	andps	%xmm15, %xmm8
	addps	%xmm8,  %xmm0
	movaps	 16 * SIZE(X), %xmm8

	andps	%xmm15, %xmm9
	addps	%xmm9,  %xmm1
	movaps	 20 * SIZE(X), %xmm9

	andps	%xmm15, %xmm10
	addps	%xmm10, %xmm2
	movaps	 24 * SIZE(X), %xmm10

	andps	%xmm15, %xmm11
	addps	%xmm11, %xmm3
	movaps	 28 * SIZE(X), %xmm11

	subq	$-32 * SIZE, X
	decq	I
	jg	.L11
	ALIGN_3

.L12:
	andps	%xmm15, %xmm4
	addps	%xmm4,  %xmm0
	andps	%xmm15, %xmm5
	addps	%xmm5,  %xmm1

	andps	%xmm15, %xmm6
	addps	%xmm6,  %xmm2
	andps	%xmm15, %xmm7
	addps	%xmm7,  %xmm3

	andps	%xmm15, %xmm8
	addps	%xmm8,  %xmm0
	andps	%xmm15, %xmm9
	addps	%xmm9,  %xmm1

	andps	%xmm15, %xmm10
	addps	%xmm10, %xmm2
	andps	%xmm15, %xmm11
	addps	%xmm11, %xmm3

	addq	$32 * SIZE, X
	ALIGN_3

.L14:
	testq	$31,  M
	jle	.L998

.L15:
	testq	$16, M
	je	.L16

	movaps	-32 * SIZE(X), %xmm4
	andps	%xmm15, %xmm4
	addps	%xmm4, %xmm0

	movaps	-28 * SIZE(X), %xmm5
	andps	%xmm15, %xmm5
	addps	%xmm5, %xmm1

	movaps	-24 * SIZE(X), %xmm4
	andps	%xmm15, %xmm4
	addps	%xmm4, %xmm0

	movaps	-20 * SIZE(X), %xmm5
	andps	%xmm15, %xmm5
	addps	%xmm5, %xmm1

	addq	$16 * SIZE, X
	ALIGN_3

.L16:
	testq	$8, M
	je	.L17

	movaps	-32 * SIZE(X), %xmm4
	andps	%xmm15, %xmm4
	addps	%xmm4, %xmm0

	movaps	-28 * SIZE(X), %xmm5
	andps	%xmm15, %xmm5
	addps	%xmm5, %xmm1

	addq	$8 * SIZE, X
	ALIGN_3

.L17:
	testq	$4, M
	je	.L18

	movaps	-32 * SIZE(X), %xmm6
	andps	%xmm15, %xmm6
	addps	%xmm6, %xmm2
	addq	$4 * SIZE, X
	ALIGN_3

.L18:
	testq	$2, M
	je	.L19

#ifdef movsd
	xorps	%xmm7, %xmm7
#endif
	movsd	-32 * SIZE(X), %xmm7
	andps	%xmm15, %xmm7
	addps	%xmm7, %xmm3
	addq	$2 * SIZE, X
	ALIGN_3

.L19:
	testq	$1, M
	je	.L998

	movss	-32 * SIZE(X), %xmm6
	andps	%xmm15, %xmm6
	addps	%xmm6, %xmm2
	jmp	.L998
	ALIGN_4

.L100:
	movq	M,  I
	sarq	$2, I
	jle	.L105
	ALIGN_4

.L101:
	movsd	(X), %xmm4
	addq	INCX, X
	movhps	(X), %xmm4
	addq	INCX, X

	andps	%xmm15, %xmm4
	addps	%xmm4,  %xmm0

	movsd	(X), %xmm5
	addq	INCX, X
	movhps	(X), %xmm5
	addq	INCX, X

	andps	%xmm15, %xmm5
	addps	%xmm5,  %xmm1

	decq	I
	jg	.L101
	ALIGN_4

.L105:
#ifdef movsd
	xorps	%xmm4, %xmm4
#endif
	andq	$3,  M
	jle	.L998
	ALIGN_4

.L106:
	movsd	(X), %xmm4
	andps	%xmm15, %xmm4
	addps	%xmm4, %xmm0
	addq	INCX, X
	decq	M
	jg	.L106
	ALIGN_4

.L998:
	addps	%xmm1, %xmm0
	addps	%xmm3, %xmm2
	addps	%xmm2, %xmm0

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
	ALIGN_4

.L999:
	RESTOREREGISTERS

	ret

	EPILOGUE
