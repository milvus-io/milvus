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
	pxor	%xmm1, %xmm1
	testq	INCX, INCX
	jle	.L999

	pxor	%xmm2, %xmm2
	leaq	(, INCX, SIZE), INCX
	pxor	%xmm3, %xmm3
	cmpq	$SIZE, INCX
	jne	.L40

	testq	$SIZE, X
	je	.L05

	movss	0 * SIZE(X), %xmm4
	cvtss2sd %xmm4, %xmm6
	mulsd	 %xmm6, %xmm6
	addsd	 %xmm6, %xmm3
	addq	INCX, X
	decq	M
	jle	.L998
	ALIGN_3

.L05:
	movq	M,  I
	sarq	$3, I
	jle	.L14

	movsd	0 * SIZE(X), %xmm4
	movsd	2 * SIZE(X), %xmm5
	movsd	4 * SIZE(X), %xmm6
	movsd	6 * SIZE(X), %xmm7
	addq	$8 * SIZE, X
	decq	I
	jle	.L12
	ALIGN_3

.L10:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	cvtps2pd %xmm4, %xmm8
	cvtps2pd %xmm5, %xmm9
	cvtps2pd %xmm6, %xmm10
	cvtps2pd %xmm7, %xmm11

	movsd	0 * SIZE(X), %xmm4
	movsd	2 * SIZE(X), %xmm5
	movsd	4 * SIZE(X), %xmm6
	movsd	6 * SIZE(X), %xmm7

	mulpd	 %xmm8,  %xmm8
	mulpd	 %xmm9,  %xmm9
	mulpd	 %xmm10, %xmm10
	mulpd	 %xmm11, %xmm11

	addpd	 %xmm8,  %xmm0
	addpd	 %xmm9,  %xmm1
	addpd	 %xmm10, %xmm2
	addpd	 %xmm11, %xmm3

	addq	$8 * SIZE, X
	decq	I
	jg	.L10
	ALIGN_3

.L12:
	cvtps2pd %xmm4, %xmm8
	cvtps2pd %xmm5, %xmm9
	cvtps2pd %xmm6, %xmm10
	cvtps2pd %xmm7, %xmm11

	mulpd	 %xmm8,  %xmm8
	mulpd	 %xmm9,  %xmm9
	mulpd	 %xmm10, %xmm10
	mulpd	 %xmm11, %xmm11

	addpd	 %xmm8,  %xmm0
	addpd	 %xmm9,  %xmm1
	addpd	 %xmm10, %xmm2
	addpd	 %xmm11, %xmm3
	ALIGN_3


.L14:
	testq	$4,  M
	je	.L15

	movsd	0 * SIZE(X), %xmm4
	movsd	2 * SIZE(X), %xmm5
	cvtps2pd %xmm4, %xmm6
	cvtps2pd %xmm5, %xmm7
	mulpd	 %xmm6, %xmm6
	mulpd	 %xmm7, %xmm7
	addpd	 %xmm6, %xmm0
	addpd	 %xmm7, %xmm1
	addq	$4 * SIZE, X
	ALIGN_3

.L15:
	testq	$2,  M
	je	.L16

	movsd	0 * SIZE(X), %xmm4
	cvtps2pd %xmm4, %xmm6
	mulpd	 %xmm6, %xmm6
	addpd	 %xmm6, %xmm2
	addq	$2 * SIZE, X
	ALIGN_3

.L16:
	testq	$1,  M
	je	.L998

	movss	0 * SIZE(X), %xmm4
	cvtss2sd %xmm4, %xmm6
	mulsd	 %xmm6, %xmm6
	addsd	 %xmm6, %xmm3
	jmp	.L998
	ALIGN_4

.L40:
	movq	M,  I
	sarq	$3, I
	jle	.L44
	ALIGN_4

.L41:
	movss	(X), %xmm4
	addq	INCX, X
	movss	(X), %xmm5
	addq	INCX, X
	movss	(X), %xmm6
	addq	INCX, X
	movss	(X), %xmm7
	addq	INCX, X
	movss	(X), %xmm8
	addq	INCX, X
	movss	(X), %xmm9
	addq	INCX, X
	movss	(X), %xmm10
	addq	INCX, X
	movss	(X), %xmm11
	addq	INCX, X

	cvtss2sd %xmm4,  %xmm4
	cvtss2sd %xmm5,  %xmm5
	cvtss2sd %xmm6,  %xmm6
	cvtss2sd %xmm7,  %xmm7
	cvtss2sd %xmm8,  %xmm8
	cvtss2sd %xmm9,  %xmm9
	cvtss2sd %xmm10, %xmm10
	cvtss2sd %xmm11, %xmm11

	mulsd	 %xmm4,  %xmm4
	mulsd	 %xmm5,  %xmm5
	mulsd	 %xmm6,  %xmm6
	mulsd	 %xmm7,  %xmm7

	addsd	 %xmm4,  %xmm0
	addsd	 %xmm5,  %xmm1
	addsd	 %xmm6,  %xmm2
	addsd	 %xmm7,  %xmm3

	mulsd	 %xmm8,  %xmm8
	mulsd	 %xmm9,  %xmm9
	mulsd	 %xmm10, %xmm10
	mulsd	 %xmm11, %xmm11

	addsd	 %xmm8,  %xmm0
	addsd	 %xmm9,  %xmm1
	addsd	 %xmm10, %xmm2
	addsd	 %xmm11, %xmm3

	decq	I
	jg	.L41
	ALIGN_3

.L44:
	testq	$4,  M
	je	.L45

	movss	(X), %xmm4
	addq	INCX, X
	movss	(X), %xmm5
	addq	INCX, X
	movss	(X), %xmm6
	addq	INCX, X
	movss	(X), %xmm7
	addq	INCX, X

	cvtss2sd %xmm4, %xmm8
	cvtss2sd %xmm5, %xmm9
	cvtss2sd %xmm6, %xmm10
	cvtss2sd %xmm7, %xmm11

	mulsd	 %xmm8,  %xmm8
	mulsd	 %xmm9,  %xmm9
	mulsd	 %xmm10, %xmm10
	mulsd	 %xmm11, %xmm11

	addsd	 %xmm8,  %xmm0
	addsd	 %xmm9,  %xmm1
	addsd	 %xmm10, %xmm2
	addsd	 %xmm11, %xmm3
	ALIGN_3

.L45:
	testq	$2,  M
	je	.L46

	movss	(X), %xmm4
	addq	INCX, X
	movss	(X), %xmm5
	addq	INCX, X

	cvtss2sd %xmm4, %xmm6
	cvtss2sd %xmm5, %xmm7
	mulsd	 %xmm6, %xmm6
	mulsd	 %xmm7, %xmm7
	addsd	 %xmm6, %xmm1
	addsd	 %xmm7, %xmm2
	ALIGN_3

.L46:
	testq	$1,  M
	je	.L998

	movss	(X), %xmm4
	cvtss2sd %xmm4, %xmm6
	mulsd	 %xmm6, %xmm6
	addsd	 %xmm6, %xmm3
	ALIGN_4

.L998:
	addpd	%xmm1, %xmm0
	addpd	%xmm3, %xmm2
	addpd	%xmm2, %xmm0

#ifndef HAVE_SSE3
	movapd	%xmm0, %xmm1
	unpckhpd	%xmm0, %xmm0
	addsd	%xmm1, %xmm0
#else
	haddpd	%xmm0, %xmm0
#endif
	ALIGN_4

.L999:
	sqrtsd	%xmm0,  %xmm0

	cvtsd2ss %xmm0, %xmm0

	RESTOREREGISTERS

	ret

	EPILOGUE

