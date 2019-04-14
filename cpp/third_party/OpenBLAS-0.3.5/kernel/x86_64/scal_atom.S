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

#ifndef WINDOWS_ABI
#define M	ARG1
#define X	ARG4
#define INCX	ARG5
#else
#define M	ARG1
#define X	ARG2
#define INCX	ARG3
#endif

#define XX	%r10
#define I	%rax

#include "l1param.h"

	PROLOGUE
	PROFCODE

#ifdef WINDOWS_ABI
	movq	40(%rsp), X
	movq	48(%rsp), INCX

	movaps	%xmm3, %xmm0
#endif

	SAVEREGISTERS

	testq	M, M
	jle	.L999

	pxor	%xmm1, %xmm1
	lea	(, INCX, SIZE), INCX
	comisd	%xmm0, %xmm1
	jne	.L100

/* Alpha == ZERO */
	cmpq	$SIZE, INCX
	jne	.L50

	movq	M,  I
	sarq	$3, I
	jle	.L12
	ALIGN_4

.L11:
	movsd	%xmm1,  0 * SIZE(X)
	movsd	%xmm1,  1 * SIZE(X)
	movsd	%xmm1,  2 * SIZE(X)
	movsd	%xmm1,  3 * SIZE(X)

	movsd	%xmm1,  4 * SIZE(X)
	movsd	%xmm1,  5 * SIZE(X)
	movsd	%xmm1,  6 * SIZE(X)
	movsd	%xmm1,  7 * SIZE(X)

	addq	$8 * SIZE, X
	decq	I
	jg	.L11
	ALIGN_4

.L12:
	testq  $4, M
	je    .L14

	movsd	%xmm1,  0 * SIZE(X)
	movsd	%xmm1,  1 * SIZE(X)
	movsd	%xmm1,  2 * SIZE(X)
	movsd	%xmm1,  3 * SIZE(X)

	addq	$4 * SIZE, X
	ALIGN_3

.L14:
	testq  $2, M
	je    .L15

	movsd	%xmm1,  0 * SIZE(X)
	movsd	%xmm1,  1 * SIZE(X)

	addq	$2 * SIZE, X
	ALIGN_3

.L15:
	testq  $1, M
	je    .L999

	movsd	%xmm1,  0 * SIZE(X)
	jmp	.L999
	ALIGN_4

.L50:
	movq	M,  I
	sarq	$3, I
	jle	.L52
	ALIGN_4

.L51:
	movsd	%xmm1, 0 * SIZE(X)
	addq	INCX, X
	movsd	%xmm1, 0 * SIZE(X)
	addq	INCX, X
	movsd	%xmm1, 0 * SIZE(X)
	addq	INCX, X
	movsd	%xmm1, 0 * SIZE(X)
	addq	INCX, X
	movsd	%xmm1, 0 * SIZE(X)
	addq	INCX, X
	movsd	%xmm1, 0 * SIZE(X)
	addq	INCX, X
	movsd	%xmm1, 0 * SIZE(X)
	addq	INCX, X
	movsd	%xmm1, 0 * SIZE(X)
	addq	INCX, X

	decq	I
	jg	.L51
	ALIGN_4

.L52:
	testq	$7, M
	je	.L999

	testq	$4, M
	je	.L53

	movsd	%xmm1, 0 * SIZE(X)
	addq	INCX, X
	movsd	%xmm1, 0 * SIZE(X)
	addq	INCX, X
	movsd	%xmm1, 0 * SIZE(X)
	addq	INCX, X
	movsd	%xmm1, 0 * SIZE(X)
	addq	INCX, X
	ALIGN_3

.L53:
	testq	$2, M
	je	.L54

	movsd	%xmm1, 0 * SIZE(X)
	addq	INCX, X
	movsd	%xmm1, 0 * SIZE(X)
	addq	INCX, X
	ALIGN_3

.L54:
	testq	$1, M
	je	.L999

	movsd	%xmm1, 0 * SIZE(X)
	jmp	.L999
	ALIGN_4

/* Alpha != ZERO */

.L100:
	cmpq	$SIZE, INCX
	jne	.L150

	unpcklpd %xmm0, %xmm0

	movq	M,  I
	sarq	$3, I
	jle	.L113

	movsd	 0 * SIZE(X), %xmm1
	movsd	 1 * SIZE(X), %xmm2
	movsd	 2 * SIZE(X), %xmm3
	movsd	 3 * SIZE(X), %xmm4
	movsd	 4 * SIZE(X), %xmm5
	movsd	 5 * SIZE(X), %xmm6
	movsd	 6 * SIZE(X), %xmm7
	movsd	 7 * SIZE(X), %xmm8

	mulsd	%xmm0, %xmm1
	mulsd	%xmm0, %xmm2
	mulsd	%xmm0, %xmm3
	mulsd	%xmm0, %xmm4

	decq	I
	jle	.L112
	ALIGN_4

.L111:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movsd	%xmm1,  0 * SIZE(X)
	movsd	%xmm2,  1 * SIZE(X)
	movsd	%xmm3,  2 * SIZE(X)
	movsd	%xmm4,  3 * SIZE(X)

	movsd	 8 * SIZE(X), %xmm1
	mulsd	%xmm0, %xmm5
	movsd	 9 * SIZE(X), %xmm2
	mulsd	%xmm0, %xmm6
	movsd	10 * SIZE(X), %xmm3
	mulsd	%xmm0, %xmm7
	movsd	11 * SIZE(X), %xmm4
	mulsd	%xmm0, %xmm8

	movsd	%xmm5,  4 * SIZE(X)
	movsd	%xmm6,  5 * SIZE(X)
	movsd	%xmm7,  6 * SIZE(X)
	movsd	%xmm8,  7 * SIZE(X)

	movsd	12 * SIZE(X), %xmm5
	mulsd	%xmm0, %xmm1
	movsd	13 * SIZE(X), %xmm6
	mulsd	%xmm0, %xmm2
	movsd	14 * SIZE(X), %xmm7
	mulsd	%xmm0, %xmm3
	movsd	15 * SIZE(X), %xmm8
	mulsd	%xmm0, %xmm4

	addq	$8 * SIZE, X
	decq	I
	jg	.L111
	ALIGN_4

.L112:
	movsd	%xmm1,  0 * SIZE(X)
	mulsd	%xmm0, %xmm5
	movsd	%xmm2,  1 * SIZE(X)
	mulsd	%xmm0, %xmm6
	movsd	%xmm3,  2 * SIZE(X)
	mulsd	%xmm0, %xmm7
	movsd	%xmm4,  3 * SIZE(X)
	mulsd	%xmm0, %xmm8

	movsd	%xmm5,  4 * SIZE(X)
	movsd	%xmm6,  5 * SIZE(X)
	movsd	%xmm7,  6 * SIZE(X)
	movsd	%xmm8,  7 * SIZE(X)
	addq	$8 * SIZE, X
	ALIGN_3

.L113:
	testq	$4, M
	je	.L115

	movsd	 0 * SIZE(X), %xmm1
	movsd	 1 * SIZE(X), %xmm2
	movsd	 2 * SIZE(X), %xmm3
	movsd	 3 * SIZE(X), %xmm4

	mulsd	%xmm0, %xmm1
	mulsd	%xmm0, %xmm2
	mulsd	%xmm0, %xmm3
	mulsd	%xmm0, %xmm4

	movsd	%xmm1,  0 * SIZE(X)
	movsd	%xmm2,  1 * SIZE(X)
	movsd	%xmm3,  2 * SIZE(X)
	movsd	%xmm4,  3 * SIZE(X)

	addq	$4 * SIZE, X
	ALIGN_3

.L115:
	testq	$2, M
	je	.L116

	movsd	 0 * SIZE(X), %xmm1
	movsd	 1 * SIZE(X), %xmm2

	mulsd	%xmm0, %xmm1
	mulsd	%xmm0, %xmm2

	movsd	%xmm1,  0 * SIZE(X)
	movsd	%xmm2,  1 * SIZE(X)

	addq	$2 * SIZE, X
	ALIGN_3

.L116:
	testq	$1, M
	je	.L999

	movsd	 0 * SIZE(X), %xmm1
	mulsd	%xmm0, %xmm1
	movsd	%xmm1,  0 * SIZE(X)
	jmp	.L999
	ALIGN_3

/* incx != 1 */

.L150:
	movq	X, XX
	movq	M,  I		# rcx = n
	sarq	$3, I		# (n >> 3)
	jle	.L152
	ALIGN_4

.L151:
	movsd	0 * SIZE(X), %xmm1
	addq	INCX, X
	movsd	0 * SIZE(X), %xmm2
	addq	INCX, X
	mulsd	%xmm0, %xmm1
	movsd	0 * SIZE(X), %xmm3
	addq	INCX, X
	mulsd	%xmm0, %xmm2
	movsd	0 * SIZE(X), %xmm4
	addq	INCX, X
	mulsd	%xmm0, %xmm3
	movsd	0 * SIZE(X), %xmm5
	addq	INCX, X
	mulsd	%xmm0, %xmm4
	movsd	0 * SIZE(X), %xmm6
	addq	INCX, X
	mulsd	%xmm0, %xmm5
	movsd	0 * SIZE(X), %xmm7
	addq	INCX, X
	mulsd	%xmm0, %xmm6
	movsd	0 * SIZE(X), %xmm8
	addq	INCX, X
	mulsd	%xmm0, %xmm7

	movsd	%xmm1, 0 * SIZE(XX)
	addq	INCX, XX
	mulsd	%xmm0, %xmm8
	movsd	%xmm2, 0 * SIZE(XX)
	addq	INCX, XX
	movsd	%xmm3, 0 * SIZE(XX)
	addq	INCX, XX
	movsd	%xmm4, 0 * SIZE(XX)
	addq	INCX, XX
	movsd	%xmm5, 0 * SIZE(XX)
	addq	INCX, XX
	movsd	%xmm6, 0 * SIZE(XX)
	addq	INCX, XX
	movsd	%xmm7, 0 * SIZE(XX)
	addq	INCX, XX
	movsd	%xmm8, 0 * SIZE(XX)
	addq	INCX, XX
	decq	I
	jg	.L151
	ALIGN_4

.L152:
	testq	$7, M
	je	.L999

	testq	$4, M
	je	.L153

	movsd	 0 * SIZE(X), %xmm1
	addq	INCX, X
	movsd	 0 * SIZE(X), %xmm2
	addq	INCX, X
	mulsd	%xmm0, %xmm1
	movsd	 0 * SIZE(X), %xmm3
	addq	INCX, X
	mulsd	%xmm0, %xmm2
	movsd	 0 * SIZE(X), %xmm4
	addq	INCX, X
	mulsd	%xmm0, %xmm3

	movsd	%xmm1,  0 * SIZE(XX)
	addq	INCX, XX
	mulsd	%xmm0, %xmm4
	movsd	%xmm2,  0 * SIZE(XX)
	addq	INCX, XX
	movsd	%xmm3,  0 * SIZE(XX)
	addq	INCX, XX
	movsd	%xmm4,  0 * SIZE(XX)
	addq	INCX, XX
	ALIGN_3

.L153:
	testq	$2, M
	je	.L154

	movsd	 0 * SIZE(X), %xmm1
	addq	INCX, X
	mulsd	%xmm0, %xmm1
	movsd	 0 * SIZE(X), %xmm2
	addq	INCX, X
	mulsd	%xmm0, %xmm2

	movsd	%xmm1,  0 * SIZE(XX)
	addq	INCX, XX
	movsd	%xmm2,  0 * SIZE(XX)
	addq	INCX, XX
	ALIGN_3

.L154:
	testq	$1, M
	je	.L999

	movsd	 0 * SIZE(X), %xmm1
	mulsd	%xmm0, %xmm1
	movsd	%xmm1,  0 * SIZE(X)
	ALIGN_4

.L999:
	xorq	%rax, %rax

	RESTOREREGISTERS

	ret

	EPILOGUE
