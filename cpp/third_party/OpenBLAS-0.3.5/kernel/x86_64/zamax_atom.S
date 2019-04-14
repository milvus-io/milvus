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

#ifdef USE_MIN
#define maxsd	minsd
#endif

#include "l1param.h"

	PROLOGUE
	PROFCODE

	SAVEREGISTERS

	xorps	%xmm0, %xmm0
	salq	$ZBASE_SHIFT, INCX

	testq	M, M
	jle	.L999

	testq	INCX, INCX
	jle	.L999

	pcmpeqb	%xmm15, %xmm15
	psrlq	$1, %xmm15

	movsd	0 * SIZE(X), %xmm0
	movsd	1 * SIZE(X), %xmm4
	addq	INCX, X

	andps	%xmm15, %xmm0
	andps	%xmm15, %xmm4

	addsd	%xmm4,  %xmm0
	decq	M
	jle	.L999

	movaps	 %xmm0, %xmm1

	cmpq	$2 * SIZE, INCX
	jne	.L20

	movq	M,  I
	sarq	$2, I
	jle	.L15

	movsd	0 * SIZE(X), %xmm4
	movsd	1 * SIZE(X), %xmm5
	movsd	2 * SIZE(X), %xmm6
	movsd	3 * SIZE(X), %xmm7

	movsd	4 * SIZE(X), %xmm8
	andps	%xmm15, %xmm4
	movsd	5 * SIZE(X), %xmm9
	andps	%xmm15, %xmm5
	movsd	6 * SIZE(X), %xmm10
	addsd	%xmm4,  %xmm5
	movsd	7 * SIZE(X), %xmm11
	decq	I
	jle	.L13
	ALIGN_4

.L12:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	andps	%xmm15, %xmm6
	movsd	 8 * SIZE(X), %xmm4
	andps	%xmm15, %xmm7
	addsd	%xmm6,  %xmm7
	movsd	10 * SIZE(X), %xmm6

	maxsd	%xmm5,  %xmm0
	movsd	 9 * SIZE(X), %xmm5
	andps	%xmm15, %xmm8
	maxsd	%xmm7,  %xmm1
	movsd	11 * SIZE(X), %xmm7
	andps	%xmm15, %xmm9
	addsd	%xmm8,  %xmm9
	movsd	12 * SIZE(X), %xmm8

	andps	%xmm15, %xmm10
	andps	%xmm15, %xmm11
	addsd	%xmm10, %xmm11
	movsd	14 * SIZE(X), %xmm10

	maxsd	%xmm9,  %xmm0
	movsd	13 * SIZE(X), %xmm9
	andps	%xmm15, %xmm4
	maxsd	%xmm11, %xmm1
	movsd	15 * SIZE(X), %xmm11
	andps	%xmm15, %xmm5
	addsd	%xmm4,  %xmm5

	addq	$8 * SIZE, X
	decq	I
	jg	.L12
	ALIGN_4

.L13:
	andps	%xmm15, %xmm6
	andps	%xmm15, %xmm7
	addsd	%xmm6,  %xmm7

	maxsd	%xmm5,  %xmm0
	andps	%xmm15, %xmm8
	maxsd	%xmm7,  %xmm1
	andps	%xmm15, %xmm9
	addsd	%xmm8,  %xmm9

	andps	%xmm15, %xmm10
	andps	%xmm15, %xmm11
	addsd	%xmm10, %xmm11

	maxsd	%xmm9,  %xmm0
	maxsd	%xmm11, %xmm1

	addq	$8 * SIZE, X
	ALIGN_4

.L15:
	testq	$2, M
	jle	.L17

	movsd	0 * SIZE(X), %xmm4
	movsd	1 * SIZE(X), %xmm5
	movsd	2 * SIZE(X), %xmm6
	movsd	3 * SIZE(X), %xmm7
	addq	$4 * SIZE, X

	andps	%xmm15, %xmm4
	andps	%xmm15, %xmm5
	addsd	%xmm4,  %xmm5

	andps	%xmm15, %xmm6
	andps	%xmm15, %xmm7
	addsd	%xmm6,  %xmm7

	maxsd	%xmm5,  %xmm0
	maxsd	%xmm7,  %xmm1
	ALIGN_3

.L17:
	testq	$1, M
	jle	.L998

	movsd	0 * SIZE(X), %xmm4
	movsd	1 * SIZE(X), %xmm5

	andps	%xmm15, %xmm4
	andps	%xmm15, %xmm5

	addsd	%xmm4,  %xmm5
	maxsd	%xmm5,  %xmm0
	jmp	.L998
	ALIGN_3

.L20:
	movq	M,  I
	sarq	$2, I
	jle	.L25

	movsd	0 * SIZE(X), %xmm4
	movsd	1 * SIZE(X), %xmm5
	addq	INCX, X
	movsd	0 * SIZE(X), %xmm6
	movsd	1 * SIZE(X), %xmm7
	addq	INCX, X

	movsd	0 * SIZE(X), %xmm8
	andps	%xmm15, %xmm4
	movsd	1 * SIZE(X), %xmm9
	addq	INCX, X
	andps	%xmm15, %xmm5
	movsd	0 * SIZE(X), %xmm10
	addsd	%xmm4,  %xmm5
	movsd	1 * SIZE(X), %xmm11
	addq	INCX, X

	decq	I
	jle	.L23
	ALIGN_4

.L22:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	andps	%xmm15, %xmm6
	movsd	 0 * SIZE(X), %xmm4
	andps	%xmm15, %xmm7
	addsd	%xmm6,  %xmm7

	maxsd	%xmm5,  %xmm0
	movsd	 1 * SIZE(X), %xmm5
	andps	%xmm15, %xmm8
	addq	INCX, X
	maxsd	%xmm7,  %xmm1
	movsd	 0 * SIZE(X), %xmm6
	andps	%xmm15, %xmm9
	movsd	 1 * SIZE(X), %xmm7
	addsd	%xmm8,  %xmm9
	addq	INCX, X

	andps	%xmm15, %xmm10
	movsd	 0 * SIZE(X), %xmm8
	andps	%xmm15, %xmm11
	addsd	%xmm10, %xmm11

	maxsd	%xmm9,  %xmm0
	movsd	 1 * SIZE(X), %xmm9
	addq	INCX, X
	andps	%xmm15, %xmm4
	movsd	 0 * SIZE(X), %xmm10
	maxsd	%xmm11, %xmm1
	movsd	 1 * SIZE(X), %xmm11
	andps	%xmm15, %xmm5
	addq	INCX, X
	addsd	%xmm4,  %xmm5

	decq	I
	jg	.L22
	ALIGN_4

.L23:
	andps	%xmm15, %xmm6
	andps	%xmm15, %xmm7
	addsd	%xmm6,  %xmm7

	maxsd	%xmm5,  %xmm0
	andps	%xmm15, %xmm8
	maxsd	%xmm7,  %xmm1
	andps	%xmm15, %xmm9
	addsd	%xmm8,  %xmm9

	andps	%xmm15, %xmm10
	andps	%xmm15, %xmm11
	addsd	%xmm10, %xmm11

	maxsd	%xmm9,  %xmm0
	maxsd	%xmm11, %xmm1
	ALIGN_4

.L25:
	testq	$2, M
	jle	.L27

	movsd	0 * SIZE(X), %xmm4
	movsd	1 * SIZE(X), %xmm5
	addq	INCX, X
	movsd	0 * SIZE(X), %xmm6
	movsd	1 * SIZE(X), %xmm7
	addq	INCX, X

	andps	%xmm15, %xmm4
	andps	%xmm15, %xmm5
	addsd	%xmm4,  %xmm5

	andps	%xmm15, %xmm6
	andps	%xmm15, %xmm7
	addsd	%xmm6,  %xmm7

	maxsd	%xmm5,  %xmm0
	maxsd	%xmm7,  %xmm1
	ALIGN_3

.L27:
	testq	$1, M
	jle	.L998

	movsd	0 * SIZE(X), %xmm4
	movsd	1 * SIZE(X), %xmm5

	andps	%xmm15, %xmm4
	andps	%xmm15, %xmm5

	addsd	%xmm4,  %xmm5
	maxsd	%xmm5,  %xmm0
	ALIGN_3

.L998:
	maxsd	%xmm1, %xmm0
	ALIGN_4

.L999:

	RESTOREREGISTERS

	ret

	EPILOGUE
