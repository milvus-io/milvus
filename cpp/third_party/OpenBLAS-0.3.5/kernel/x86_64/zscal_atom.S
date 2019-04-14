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
	movaps	%xmm3, %xmm0
	movsd	40(%rsp), %xmm1
	movq	48(%rsp), X
	movq	56(%rsp), INCX
#endif

	SAVEREGISTERS

	salq	$ZBASE_SHIFT, INCX

	testq	M, M
	jle	.L999

	pxor	%xmm15, %xmm15
	comisd	%xmm0, %xmm15
	jne	.L30		# Alpha_r != ZERO

	comisd	%xmm1, %xmm15
	jne	.L30		# Alpha_i != ZERO


/* Alpha == ZERO */
	cmpq	$2 * SIZE, INCX
	jne	.L20

	movq	M,  I
	sarq	$2, I
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
	testq  $2, M
	je    .L14

	movsd	%xmm1,  0 * SIZE(X)
	movsd	%xmm1,  1 * SIZE(X)
	movsd	%xmm1,  2 * SIZE(X)
	movsd	%xmm1,  3 * SIZE(X)

	addq	$4 * SIZE, X
	ALIGN_3

.L14:
	testq  $1, M
	je    .L999

	movsd	%xmm1,  0 * SIZE(X)
	movsd	%xmm1,  1 * SIZE(X)
	addq	$2 * SIZE, X
	jmp	.L999
	ALIGN_4

.L20:
	movq	M,  I		# rcx = n
	sarq	$2, I
	jle	.L22
	ALIGN_4

.L21:
	movsd	%xmm1, 0 * SIZE(X)
	movsd	%xmm1, 1 * SIZE(X)
	addq	INCX, X
	movsd	%xmm1, 0 * SIZE(X)
	movsd	%xmm1, 1 * SIZE(X)
	addq	INCX, X
	movsd	%xmm1, 0 * SIZE(X)
	movsd	%xmm1, 1 * SIZE(X)
	addq	INCX, X
	movsd	%xmm1, 0 * SIZE(X)
	movsd	%xmm1, 1 * SIZE(X)
	addq	INCX, X
	decq	I
	jg	.L21
	ALIGN_4

.L22:
	testq	$2, M
	je	.L23

	movsd	%xmm1, 0 * SIZE(X)
	movsd	%xmm1, 1 * SIZE(X)
	addq	INCX, X
	movsd	%xmm1, 0 * SIZE(X)
	movsd	%xmm1, 1 * SIZE(X)
	addq	INCX, X
	ALIGN_3

.L23:
	testq	$1, M
	je	.L999

	movsd	%xmm1, 0 * SIZE(X)
	movsd	%xmm1, 1 * SIZE(X)
	jmp	.L999
	ALIGN_4

/* Alpha != ZERO */
.L30:
	movq	X, XX

	movq	M,  I
	sarq	$2, I
	jle	.L35

	movsd	 0 * SIZE(X), %xmm2
	movsd	 1 * SIZE(X), %xmm3
	addq	INCX, X
	movsd	 0 * SIZE(X), %xmm6
	movsd	 1 * SIZE(X), %xmm7
	addq	INCX, X

	movaps	%xmm2, %xmm4
	movsd	 0 * SIZE(X), %xmm8
	mulsd	%xmm0, %xmm2
	movaps	%xmm3, %xmm5
	movsd	 1 * SIZE(X), %xmm9
	mulsd	%xmm1, %xmm5
	addq	INCX, X
	mulsd	%xmm0, %xmm3
	mulsd	%xmm1, %xmm4

	subsd	%xmm5,  %xmm2
	movsd	 0 * SIZE(X), %xmm10
	addsd	%xmm4,  %xmm3
	movsd	 1 * SIZE(X), %xmm11

	movaps	%xmm6, %xmm4
	mulsd	%xmm0, %xmm6
	addq	INCX, X
	movaps	%xmm7, %xmm5
	mulsd	%xmm1, %xmm5
	mulsd	%xmm0, %xmm7
	mulsd	%xmm1, %xmm4

	decq	  I
	jle	  .L32
	ALIGN_4

.L31:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	subsd	%xmm5,  %xmm6
	movsd	%xmm2,  0 * SIZE(XX)
	addsd	%xmm4,  %xmm7
	movsd	%xmm3,  1 * SIZE(XX)

	movaps	%xmm8, %xmm4
	movsd	 0 * SIZE(X), %xmm2
	mulsd	%xmm0, %xmm8
	addq	INCX, XX
	movaps	%xmm9, %xmm5
	movsd	 1 * SIZE(X), %xmm3
	mulsd	%xmm1, %xmm5
	addq	INCX, X
	mulsd	%xmm0, %xmm9
	mulsd	%xmm1, %xmm4

	subsd	%xmm5,  %xmm8
	movsd	%xmm6,  0 * SIZE(XX)
	addsd	%xmm4,  %xmm9
	movsd	%xmm7,  1 * SIZE(XX)

	movaps	%xmm10, %xmm4
	movsd	0 * SIZE(X), %xmm6
	mulsd	%xmm0,  %xmm10
	addq	INCX, XX
	movaps	%xmm11, %xmm5
	movsd	1 * SIZE(X), %xmm7
	mulsd	%xmm1,  %xmm5
	addq	INCX, X
	mulsd	%xmm0,  %xmm11
	mulsd	%xmm1,  %xmm4

	subsd	%xmm5,  %xmm10
	movsd	%xmm8,  0 * SIZE(XX)
	addsd	%xmm4,  %xmm11
	movsd	%xmm9,  1 * SIZE(XX)

	movaps	%xmm2, %xmm4
	movsd	0 * SIZE(X), %xmm8
	mulsd	%xmm0, %xmm2
	addq	INCX, XX
	movaps	%xmm3, %xmm5
	movsd	1 * SIZE(X), %xmm9
	mulsd	%xmm1, %xmm5
	addq	INCX, X
	mulsd	%xmm0, %xmm3
	mulsd	%xmm1, %xmm4

	subsd	%xmm5,  %xmm2
	movsd	%xmm10, 0 * SIZE(XX)
	addsd	%xmm4,  %xmm3
	movsd	%xmm11, 1 * SIZE(XX)

	movaps	%xmm6, %xmm4
 	movsd	0 * SIZE(X), %xmm10
	mulsd	%xmm0, %xmm6
	addq	INCX, XX
	movaps	%xmm7, %xmm5
	movsd	1 * SIZE(X), %xmm11
	mulsd	%xmm1, %xmm5
	addq	INCX, X
	mulsd	%xmm0, %xmm7
	mulsd	%xmm1, %xmm4

	decq	I
	jg	.L31
	ALIGN_4

.L32:
	subsd	%xmm5,  %xmm6
	movsd	%xmm2,  0 * SIZE(XX)
	addsd	%xmm4,  %xmm7
	movsd	%xmm3,  1 * SIZE(XX)

	movaps	%xmm8, %xmm4
	mulsd	%xmm0, %xmm8
	addq	INCX, XX
	movaps	%xmm9, %xmm5
	mulsd	%xmm1, %xmm5
	mulsd	%xmm0, %xmm9
	mulsd	%xmm1, %xmm4

	subsd	%xmm5,  %xmm8
	movsd	%xmm6,  0 * SIZE(XX)
	addsd	%xmm4,  %xmm9
	movsd	%xmm7,  1 * SIZE(XX)

	movaps	%xmm10, %xmm4
	mulsd	%xmm0,  %xmm10
	addq	INCX, XX
	movaps	%xmm11, %xmm5
	mulsd	%xmm1,  %xmm5
	mulsd	%xmm0,  %xmm11
	mulsd	%xmm1,  %xmm4

	subsd	%xmm5,  %xmm10
	movsd	%xmm8,  0 * SIZE(XX)
	addsd	%xmm4,  %xmm11
	movsd	%xmm9,  1 * SIZE(XX)
	addq	INCX, XX

	movsd	%xmm10, 0 * SIZE(XX)
	movsd	%xmm11, 1 * SIZE(XX)
	addq	INCX, XX
	ALIGN_3

.L35:
	testq	$2, M
	je	.L37

	movsd	 0 * SIZE(X), %xmm2
	movsd	 1 * SIZE(X), %xmm3
	addq	INCX, X

	movaps	%xmm2, %xmm4
	movsd	 0 * SIZE(X), %xmm6
	mulsd	%xmm0, %xmm2
	movaps	%xmm3, %xmm5
	movsd	 1 * SIZE(X), %xmm7
	mulsd	%xmm1, %xmm5
	addq	INCX, X
	mulsd	%xmm0, %xmm3
	mulsd	%xmm1, %xmm4

	subsd	%xmm5,  %xmm2
	addsd	%xmm4,  %xmm3

	movaps	%xmm6, %xmm4
	mulsd	%xmm0, %xmm6
	movaps	%xmm7, %xmm5
	mulsd	%xmm1, %xmm5
	mulsd	%xmm0, %xmm7
	mulsd	%xmm1, %xmm4

	subsd	%xmm5,  %xmm6
	movsd	%xmm2,  0 * SIZE(XX)
	addsd	%xmm4,  %xmm7
	movsd	%xmm3,  1 * SIZE(XX)
	addq	INCX, XX

	movsd	%xmm6,  0 * SIZE(XX)
	movsd	%xmm7,  1 * SIZE(XX)
	addq	INCX, XX
	ALIGN_3

.L37:
	testq	$1, M
	je	.L999

	movsd	 0 * SIZE(X), %xmm2
	movsd	 1 * SIZE(X), %xmm3

	movaps	%xmm2, %xmm4
	mulsd	%xmm0, %xmm2
	movaps	%xmm3, %xmm5
	mulsd	%xmm1, %xmm5
	mulsd	%xmm0, %xmm3
	mulsd	%xmm1, %xmm4

	subsd	%xmm5,  %xmm2
	addsd	%xmm4,  %xmm3

	movsd	%xmm2,  0 * SIZE(XX)
	movsd	%xmm3,  1 * SIZE(XX)
	ALIGN_3

.L999:
	xorq	%rax, %rax

	RESTOREREGISTERS

	ret

	EPILOGUE

