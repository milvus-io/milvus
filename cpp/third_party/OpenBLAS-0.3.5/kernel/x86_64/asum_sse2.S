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

	xorps	%xmm0, %xmm0
	testq	M, M
	jle	.L999
	testq	INCX, INCX
	jle	.L999

	xorps	%xmm1, %xmm1
	xorps	%xmm2, %xmm2
	xorps	%xmm3, %xmm3

	pcmpeqb	%xmm15, %xmm15
	psrlq	$1, %xmm15

	salq	$BASE_SHIFT, INCX

	subq	$-16 * SIZE, X

	cmpq	$SIZE, INCX
	jne	.L40

	testq	$SIZE, X
	je	.L05

	movsd	-16 * SIZE(X), %xmm0
	addq	$SIZE, X

	andps	%xmm15, %xmm0
	subq	$1, M
	jle	.L999
	ALIGN_3

.L05:
	movq	M,  I
	sarq	$4, I
	jle	.L20

	movaps	-16 * SIZE(X), %xmm4
	movaps	-14 * SIZE(X), %xmm5
	movaps	-12 * SIZE(X), %xmm6
	movaps	-10 * SIZE(X), %xmm7

	movaps	 -8 * SIZE(X), %xmm8
	movaps	 -6 * SIZE(X), %xmm9
	movaps	 -4 * SIZE(X), %xmm10
	movaps	 -2 * SIZE(X), %xmm11

	decq	I
	jle	.L11
	ALIGN_4

.L10:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	andps	%xmm15, %xmm4
	addpd	%xmm4,  %xmm0
	movaps	  0 * SIZE(X), %xmm4

	andps	%xmm15, %xmm5
	addpd	%xmm5,  %xmm1
	movaps	  2 * SIZE(X), %xmm5

	andps	%xmm15, %xmm6
	addpd	%xmm6,  %xmm2
	movaps	  4 * SIZE(X), %xmm6

	andps	%xmm15, %xmm7
	addpd	%xmm7,  %xmm3
	movaps	  6 * SIZE(X), %xmm7

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	andps	%xmm15, %xmm8
	addpd	%xmm8,  %xmm0
	movaps	  8 * SIZE(X), %xmm8

	andps	%xmm15, %xmm9
	addpd	%xmm9,  %xmm1
	movaps	 10 * SIZE(X), %xmm9

	andps	%xmm15, %xmm10
	addpd	%xmm10, %xmm2
	movaps	 12 * SIZE(X), %xmm10

	andps	%xmm15, %xmm11
	addpd	%xmm11, %xmm3
	movaps	 14 * SIZE(X), %xmm11

	subq	$-16 * SIZE, X
	decq	I
	jg	.L10
	ALIGN_4

.L11:
	andps	%xmm15, %xmm4
	andps	%xmm15, %xmm5
	andps	%xmm15, %xmm6
	andps	%xmm15, %xmm7

	addpd	%xmm4,  %xmm0
	addpd	%xmm5,  %xmm1
	addpd	%xmm6,  %xmm2
	addpd	%xmm7,  %xmm3

	andps	%xmm15, %xmm8
	andps	%xmm15, %xmm9
	andps	%xmm15, %xmm10
	andps	%xmm15, %xmm11

	addpd	%xmm8,  %xmm0
	addpd	%xmm9,  %xmm1
	addpd	%xmm10, %xmm2
	addpd	%xmm11, %xmm3

	subq	$-16 * SIZE, X
	ALIGN_3

.L20:
	andq	$15,  M
	jle	.L998

	testq	$8, M
	je	.L21

	movaps	-16 * SIZE(X), %xmm4
	movaps	-14 * SIZE(X), %xmm5
	movaps	-12 * SIZE(X), %xmm6
	movaps	-10 * SIZE(X), %xmm7

	andps	%xmm15, %xmm4
	andps	%xmm15, %xmm5
	andps	%xmm15, %xmm6
	andps	%xmm15, %xmm7

	addpd	%xmm4, %xmm0
	addpd	%xmm5, %xmm1
	addpd	%xmm6, %xmm2
	addpd	%xmm7, %xmm3
	addq	$8 * SIZE, X
	ALIGN_3

.L21:
	testq	$4, M
	je	.L22

	movaps	-16 * SIZE(X), %xmm4
	movaps	-14 * SIZE(X), %xmm5

	andps	%xmm15, %xmm4
	andps	%xmm15, %xmm5
	addpd	%xmm4, %xmm0
	addpd	%xmm5, %xmm1

	addq	$4 * SIZE, X
	ALIGN_3

.L22:
	testq	$2, M
	je	.L23

	movaps	-16 * SIZE(X), %xmm6
	andps	%xmm15, %xmm6
	addpd	%xmm6, %xmm3
	addq	$2 * SIZE, X

.L23:
	testq	$1, M
	je	.L998

#ifdef movsd
	xorps	%xmm4, %xmm4
#endif
	movsd	-16 * SIZE(X), %xmm4
	andps	%xmm15, %xmm4
	addsd	%xmm4, %xmm0
	jmp	.L998
	ALIGN_3

.L40:
	movq	M,  I
	sarq	$3, I
	jle	.L60
	ALIGN_4

.L50:
	movsd	-16 * SIZE(X), %xmm4
	addq	INCX, X
	movhpd	-16 * SIZE(X), %xmm4
	addq	INCX, X
	andps	%xmm15, %xmm4
	addpd	%xmm4, %xmm0

	movsd	-16 * SIZE(X), %xmm5
	addq	INCX, X
	movhpd	-16 * SIZE(X), %xmm5
	addq	INCX, X
	andps	%xmm15, %xmm5
	addpd	%xmm5, %xmm1

	movsd	-16 * SIZE(X), %xmm6
	addq	INCX, X
	movhpd	-16 * SIZE(X), %xmm6
	addq	INCX, X
	andps	%xmm15, %xmm6
	addpd	%xmm6, %xmm2

	movsd	-16 * SIZE(X), %xmm7
	addq	INCX, X
	movhpd	-16 * SIZE(X), %xmm7
	addq	INCX, X
	andps	%xmm15, %xmm7
	addpd	%xmm7, %xmm3

	decq	I
	jg	.L50
	ALIGN_4

.L60:
#ifdef movsd
	xorps	%xmm4, %xmm4
#endif
	andq	$7,  M
	jle	.L998
	ALIGN_4

.L61:
	movsd	-16 * SIZE(X), %xmm4
	andps	%xmm15, %xmm4
	addpd	%xmm4, %xmm0
	addq	INCX, X
	decq	M
	jg	.L61
	ALIGN_4

.L998:
	addpd	%xmm1, %xmm0
	addpd	%xmm3, %xmm2
	addpd	%xmm2, %xmm0
	ALIGN_4

.L999:
#ifndef HAVE_SSE3
	movhlps	%xmm0, %xmm1
	addsd	%xmm1, %xmm0
#else
	haddpd	%xmm0, %xmm0
#endif

	RESTOREREGISTERS

	ret

	EPILOGUE

