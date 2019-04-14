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
#define ARGS	 0

#define STACK_M		 4 + STACK + ARGS(%esp)
#define STACK_X		 8 + STACK + ARGS(%esp)
#define STACK_INCX	12 + STACK + ARGS(%esp)

#define M	%edx
#define X	%ecx
#define INCX	%esi

#define I	%eax

#include "l1param.h"

	PROLOGUE
	PROFCODE

	pushl	%esi
	pushl	%ebx

	movl	STACK_M,    M
	movl	STACK_X,    X
	movl	STACK_INCX, INCX

	pxor	%xmm0, %xmm0
	testl	M, M
	jle	.L999
	pxor	%xmm1, %xmm1
	testl	INCX, INCX
	jle	.L999

	leal	(, INCX, SIZE), INCX
	cmpl	$SIZE, INCX
	jne	.L40

	subl	$-32 * SIZE, X

	testl	$SIZE, X
	je	.L05

	movss	-32 * SIZE(X), %xmm0
	cvtss2sd %xmm0, %xmm0
	mulsd	 %xmm0, %xmm0

	addl	INCX, X
	decl	M
	jle	.L998
	ALIGN_3

.L05:
	movl	M,  I
	sarl	$4, I
	jle	.L13

	movsd	-32 * SIZE(X), %xmm4
	movsd	-30 * SIZE(X), %xmm5
	movsd	-28 * SIZE(X), %xmm6
	movsd	-26 * SIZE(X), %xmm7

	decl	I
	jle	.L12
	ALIGN_3

.L10:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	cvtps2pd %xmm4, %xmm2
	movsd	-24 * SIZE(X), %xmm4
	mulpd	 %xmm2, %xmm2
	addpd	 %xmm2, %xmm0

	cvtps2pd %xmm5, %xmm3
	movsd	-22 * SIZE(X), %xmm5
	mulpd	 %xmm3, %xmm3
	addpd	 %xmm3, %xmm1

	cvtps2pd %xmm6, %xmm2
	movsd	-20 * SIZE(X), %xmm6
	mulpd	 %xmm2, %xmm2
	addpd	 %xmm2, %xmm0

	cvtps2pd %xmm7, %xmm3
	movsd	-18 * SIZE(X), %xmm7
	mulpd	 %xmm3, %xmm3
	addpd	 %xmm3, %xmm1

	cvtps2pd %xmm4, %xmm2
	movsd	-16 * SIZE(X), %xmm4
	mulpd	 %xmm2, %xmm2
	addpd	 %xmm2, %xmm0

	cvtps2pd %xmm5, %xmm3
	movsd	-14 * SIZE(X), %xmm5
	mulpd	 %xmm3, %xmm3
	addpd	 %xmm3, %xmm1

	cvtps2pd %xmm6, %xmm2
	movsd	-12 * SIZE(X), %xmm6
	mulpd	 %xmm2, %xmm2
	addpd	 %xmm2, %xmm0

	cvtps2pd %xmm7, %xmm3
	movsd	-10 * SIZE(X), %xmm7
	mulpd	 %xmm3, %xmm3
	addpd	 %xmm3, %xmm1

	subl	$-16 * SIZE, X
	decl	I
	jg	.L10
	ALIGN_3

.L12:
	cvtps2pd %xmm4, %xmm2
	movsd	-24 * SIZE(X), %xmm4
	mulpd	 %xmm2, %xmm2
	addpd	 %xmm2, %xmm0

	cvtps2pd %xmm5, %xmm3
	movsd	-22 * SIZE(X), %xmm5
	mulpd	 %xmm3, %xmm3
	addpd	 %xmm3, %xmm1

	cvtps2pd %xmm6, %xmm2
	movsd	-20 * SIZE(X), %xmm6
	mulpd	 %xmm2, %xmm2
	addpd	 %xmm2, %xmm0

	cvtps2pd %xmm7, %xmm3
	movsd	-18 * SIZE(X), %xmm7
	mulpd	 %xmm3, %xmm3
	addpd	 %xmm3, %xmm1

	cvtps2pd %xmm4, %xmm2
	mulpd	 %xmm2, %xmm2
	addpd	 %xmm2, %xmm0

	cvtps2pd %xmm5, %xmm3
	mulpd	 %xmm3, %xmm3
	addpd	 %xmm3, %xmm1

	cvtps2pd %xmm6, %xmm2
	mulpd	 %xmm2, %xmm2
	addpd	 %xmm2, %xmm0

	cvtps2pd %xmm7, %xmm3
	mulpd	 %xmm3, %xmm3
	addpd	 %xmm3, %xmm1

	subl	$-16 * SIZE, X
	ALIGN_4

.L13:
	testl	$8,  M
	je	.L14

	movsd	-32 * SIZE(X), %xmm4

	cvtps2pd %xmm4, %xmm2
	mulpd	 %xmm2, %xmm2
	addpd	 %xmm2, %xmm0

	movsd	-30 * SIZE(X), %xmm5

	cvtps2pd %xmm5, %xmm3
	mulpd	 %xmm3, %xmm3
	addpd	 %xmm3, %xmm1

	movsd	-28 * SIZE(X), %xmm6

	cvtps2pd %xmm6, %xmm2
	mulpd	 %xmm2, %xmm2
	addpd	 %xmm2, %xmm0

	movsd	-26 * SIZE(X), %xmm7

	cvtps2pd %xmm7, %xmm3
	mulpd	 %xmm3, %xmm3
	addpd	 %xmm3, %xmm1

	addl	$8 * SIZE, X
	ALIGN_3

.L14:
	testl	$4,  M
	je	.L15

	movsd	-32 * SIZE(X), %xmm4
	cvtps2pd %xmm4, %xmm2
	mulpd	 %xmm2, %xmm2
	addpd	 %xmm2, %xmm0

	movsd	-30 * SIZE(X), %xmm5
	cvtps2pd %xmm5, %xmm3
	mulpd	 %xmm3, %xmm3
	addpd	 %xmm3, %xmm1
	addl	$4 * SIZE, X
	ALIGN_3

.L15:
	testl	$2,  M
	je	.L16

	movsd	-32 * SIZE(X), %xmm4
	cvtps2pd %xmm4, %xmm2
	mulpd	 %xmm2, %xmm2
	addpd	 %xmm2, %xmm0
	addl	$2 * SIZE, X
	ALIGN_3

.L16:
	testl	$1,  M
	je	.L998

	movss	-32 * SIZE(X), %xmm4
	cvtss2sd %xmm4, %xmm2
	mulsd	 %xmm2, %xmm2
	addsd	 %xmm2, %xmm1
	jmp	.L998
	ALIGN_4

.L40:
	movl	M,  I
	sarl	$3, I
	jle	.L44
	ALIGN_4

.L41:
	movss	(X), %xmm4
	addl	INCX, X

	cvtss2sd %xmm4, %xmm2
	mulsd	 %xmm2, %xmm2
	addsd	 %xmm2, %xmm0

	movss	(X), %xmm5
	addl	INCX, X

	cvtss2sd %xmm5, %xmm2
	mulsd	 %xmm2, %xmm2
	addsd	 %xmm2, %xmm1

	movss	(X), %xmm6
	addl	INCX, X

	cvtss2sd %xmm6, %xmm2
	mulsd	 %xmm2, %xmm2
	addsd	 %xmm2, %xmm0

	movss	(X), %xmm7
	addl	INCX, X

	cvtss2sd %xmm7, %xmm2
	mulsd	 %xmm2, %xmm2
	addsd	 %xmm2, %xmm1

	movss	(X), %xmm4
	addl	INCX, X

	cvtss2sd %xmm4, %xmm2
	mulsd	 %xmm2, %xmm2
	addsd	 %xmm2, %xmm0

	movss	(X), %xmm5
	addl	INCX, X

	cvtss2sd %xmm5, %xmm2
	mulsd	 %xmm2, %xmm2
	addsd	 %xmm2, %xmm1

	movss	(X), %xmm6
	addl	INCX, X

	cvtss2sd %xmm6, %xmm2
	mulsd	 %xmm2, %xmm2
	addsd	 %xmm2, %xmm0

	movss	(X), %xmm7
	addl	INCX, X

	cvtss2sd %xmm7, %xmm2
	mulsd	 %xmm2, %xmm2
	addsd	 %xmm2, %xmm1

	decl	I
	jg	.L41
	ALIGN_3

.L44:
	testl	$4,  M
	je	.L45

	movss	(X), %xmm4
	addl	INCX, X

	cvtss2sd %xmm4, %xmm2
	mulsd	 %xmm2, %xmm2
	addsd	 %xmm2, %xmm0

	movss	(X), %xmm5
	addl	INCX, X

	cvtss2sd %xmm5, %xmm2
	mulsd	 %xmm2, %xmm2
	addsd	 %xmm2, %xmm1

	movss	(X), %xmm6
	addl	INCX, X

	cvtss2sd %xmm6, %xmm2
	mulsd	 %xmm2, %xmm2
	addsd	 %xmm2, %xmm0

	movss	(X), %xmm7
	addl	INCX, X

	cvtss2sd %xmm7, %xmm2
	mulsd	 %xmm2, %xmm2
	addsd	 %xmm2, %xmm1
	ALIGN_3

.L45:
	testl	$2,  M
	je	.L46

	movss	(X), %xmm4
	addl	INCX, X

	cvtss2sd %xmm4, %xmm2
	mulsd	 %xmm2, %xmm2
	addsd	 %xmm2, %xmm0

	movss	(X), %xmm5
	addl	INCX, X

	cvtss2sd %xmm5, %xmm2
	mulsd	 %xmm2, %xmm2
	addsd	 %xmm2, %xmm1
	ALIGN_3

.L46:
	testl	$1,  M
	je	.L998

	movss	(X), %xmm4
	cvtss2sd %xmm4, %xmm2
	mulsd	 %xmm2, %xmm2
	addsd	 %xmm2, %xmm0
	ALIGN_4

.L998:
	addpd	%xmm1, %xmm0

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

	movss	 %xmm0, STACK_M
	flds	 STACK_M

	popl	%ebx
	popl	%esi
	ret

	EPILOGUE
