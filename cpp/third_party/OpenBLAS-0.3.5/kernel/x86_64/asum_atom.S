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
	xorps	 %xmm13, %xmm13

	cmpq	$SIZE, INCX
	jne	.L20

	testq	$SIZE, X
	je	.L05

	movsd	(X), %xmm0
	addq	$SIZE, X
	andps	%xmm15, %xmm0
	decq	M
	jle	.L999
	ALIGN_3

.L05:
	subq	$-16 * SIZE, X

	movq	M,  I
	sarq	$4, I
	jle	.L12

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
	addsd	%xmm13, %xmm3
	pshufd	$0x4e, %xmm4, %xmm12
	addsd	%xmm4, %xmm0
	movaps	  0 * SIZE(X), %xmm4

	andps	%xmm15, %xmm5
	addsd	%xmm12, %xmm1
	pshufd	$0x4e, %xmm5, %xmm13
	addsd	%xmm5, %xmm2
	movaps	  2 * SIZE(X), %xmm5

	andps	%xmm15, %xmm6
	addsd	%xmm13, %xmm3
	pshufd	$0x4e, %xmm6, %xmm12
	addsd	%xmm6, %xmm0
	movaps	  4 * SIZE(X), %xmm6

	andps	%xmm15, %xmm7
	addsd	%xmm12, %xmm1
	pshufd	$0x4e, %xmm7, %xmm13
	addsd	%xmm7, %xmm2
	movaps	  6 * SIZE(X), %xmm7

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	andps	%xmm15, %xmm8
	addsd	%xmm13, %xmm3
	pshufd	$0x4e, %xmm8, %xmm12
	addsd	%xmm8, %xmm0
	movaps	  8 * SIZE(X), %xmm8

	andps	%xmm15, %xmm9
	addsd	%xmm12, %xmm1
	pshufd	$0x4e, %xmm9, %xmm13
	addsd	%xmm9, %xmm2
	movaps	 10 * SIZE(X), %xmm9

	andps	%xmm15, %xmm10
	addsd	%xmm13, %xmm3
	pshufd	$0x4e, %xmm10, %xmm12
	addsd	%xmm10, %xmm0
	movaps	 12 * SIZE(X), %xmm10

	andps	%xmm15, %xmm11
	addsd	%xmm12, %xmm1
	pshufd	$0x4e, %xmm11, %xmm13
	addsd	%xmm11, %xmm2
	movaps	 14 * SIZE(X), %xmm11

	subq	$-16 * SIZE, X
	decq	I
	jg	.L10
	ALIGN_4

.L11:
	andps	%xmm15, %xmm4
	addsd	%xmm13, %xmm3
	pshufd	$0x4e, %xmm4, %xmm12
	addsd	%xmm4, %xmm0

	andps	%xmm15, %xmm5
	addsd	%xmm12, %xmm1
	pshufd	$0x4e, %xmm5, %xmm13
	addsd	%xmm5, %xmm2

	andps	%xmm15, %xmm6
	addsd	%xmm13, %xmm3
	pshufd	$0x4e, %xmm6, %xmm12
	addsd	%xmm6, %xmm0

	andps	%xmm15, %xmm7
	addsd	%xmm12, %xmm1
	pshufd	$0x4e, %xmm7, %xmm13
	addsd	%xmm7, %xmm2

	andps	%xmm15, %xmm8
	addsd	%xmm13, %xmm3
	pshufd	$0x4e, %xmm8, %xmm12
	addsd	%xmm8, %xmm0

	andps	%xmm15, %xmm9
	addsd	%xmm12, %xmm1
	pshufd	$0x4e, %xmm9, %xmm13
	addsd	%xmm9, %xmm2

	andps	%xmm15, %xmm10
	addsd	%xmm13, %xmm3
	pshufd	$0x4e, %xmm10, %xmm12
	addsd	%xmm10, %xmm0

	andps	%xmm15, %xmm11
	addsd	%xmm12, %xmm1
	pshufd	$0x4e, %xmm11, %xmm13
	addsd	%xmm11, %xmm2

	addsd	%xmm13, %xmm3
	subq	$-16 * SIZE, X
	ALIGN_3

.L12:
	andq	$15,  M
	jle	.L998

	testq	$8, M
	je	.L13

	movaps	-16 * SIZE(X), %xmm4
	movaps	-14 * SIZE(X), %xmm5
	movaps	-12 * SIZE(X), %xmm6
	movaps	-10 * SIZE(X), %xmm7
	addq	$8 * SIZE, X

	andps	%xmm15, %xmm4
	pshufd	$0x4e, %xmm4, %xmm12
	addsd	%xmm4, %xmm0
	andps	%xmm15, %xmm5
	addsd	%xmm12, %xmm1
	pshufd	$0x4e, %xmm5, %xmm13
	addsd	%xmm5, %xmm2
	addsd	%xmm13, %xmm3
	andps	%xmm15, %xmm6
	pshufd	$0x4e, %xmm6, %xmm12
	addsd	%xmm6, %xmm0
	andps	%xmm15, %xmm7
	addsd	%xmm12, %xmm1
	pshufd	$0x4e, %xmm7, %xmm13
	addsd	%xmm7, %xmm2
	addsd	%xmm13, %xmm3
	ALIGN_3

.L13:
	testq	$4, M
	je	.L14

	movaps	-16 * SIZE(X), %xmm4
	movaps	-14 * SIZE(X), %xmm5
	addq	$4 * SIZE, X

	andps	%xmm15, %xmm4
	pshufd	$0x4e, %xmm4, %xmm12
	addsd	%xmm4, %xmm0
	andps	%xmm15, %xmm5
	addsd	%xmm12, %xmm1
	pshufd	$0x4e, %xmm5, %xmm13
	addsd	%xmm5, %xmm2
	addsd	%xmm13, %xmm3
	ALIGN_3

.L14:
	testq	$2, M
	je	.L15

	movaps	-16 * SIZE(X), %xmm4
	addq	$2 * SIZE, X
	andps	%xmm15, %xmm4

	pshufd	$0x4e, %xmm4, %xmm5
	addsd	%xmm4, %xmm2
	addsd	%xmm5, %xmm3
	ALIGN_3

.L15:
	testq	$1, M
	je	.L998

	movsd	-16 * SIZE(X), %xmm4
	andps	%xmm15, %xmm4
	addsd	%xmm4, %xmm0
	jmp	.L998
	ALIGN_3

.L20:
	movq	M,  I
	sarq	$3, I
	jle	.L25

	movsd	(X), %xmm4
	addq	INCX, X
	movsd	(X), %xmm5
	addq	INCX, X
	movsd	(X), %xmm6
	addq	INCX, X
	movsd	(X), %xmm7
	addq	INCX, X

	movsd	(X), %xmm8
	addq	INCX, X
	movsd	(X), %xmm9
	addq	INCX, X
	movsd	(X), %xmm10
	addq	INCX, X
	movsd	(X), %xmm11

	decq	I
	jle	.L23
	ALIGN_4

.L22:
	andps	%xmm15, %xmm4
	addq	INCX, X
	addsd	%xmm4,  %xmm0
	movsd	 (X), %xmm4
	andps	%xmm15, %xmm5
	addq	INCX, X
	addsd	%xmm5,  %xmm1
	movsd	 (X), %xmm5
	andps	%xmm15, %xmm6
	addq	INCX, X
	addsd	%xmm6,  %xmm2
	movsd	 (X), %xmm6
	andps	%xmm15, %xmm7
	addq	INCX, X
	addsd	%xmm7,  %xmm3
	movsd	 (X), %xmm7

	andps	%xmm15, %xmm8
	addq	INCX, X
	addsd	%xmm8,  %xmm0
	movsd	 (X), %xmm8
	andps	%xmm15, %xmm9
	addq	INCX, X
	addsd	%xmm9,  %xmm1
	movsd	 (X), %xmm9
	andps	%xmm15, %xmm10
	addq	INCX, X
	addsd	%xmm10, %xmm2
	movsd	 (X), %xmm10
	andps	%xmm15, %xmm11
	addq	INCX, X
	addsd	%xmm11, %xmm3
	movsd	 (X), %xmm11

	decq	I
	jg	.L22
	ALIGN_4

.L23:
	andps	%xmm15, %xmm4
	addq	INCX, X
	addsd	%xmm4,  %xmm0
	andps	%xmm15, %xmm5
	addsd	%xmm5,  %xmm1
	andps	%xmm15, %xmm6
	addsd	%xmm6,  %xmm2
	andps	%xmm15, %xmm7
	addsd	%xmm7,  %xmm3

	andps	%xmm15, %xmm8
	addsd	%xmm8,  %xmm0
	andps	%xmm15, %xmm9
	addsd	%xmm9,  %xmm1
	andps	%xmm15, %xmm10
	addsd	%xmm10, %xmm2
	andps	%xmm15, %xmm11
	addsd	%xmm11, %xmm3
	ALIGN_3

.L25:
	andq	$7,  M
	jle	.L998

	testq	$4, M
	je	.L26

	movsd	(X), %xmm4
	addq	INCX, X
	movsd	(X), %xmm5
	addq	INCX, X
	movsd	(X), %xmm6
	andps	%xmm15, %xmm4
	addsd	%xmm4,  %xmm0
	addq	INCX, X
	movsd	(X), %xmm7
	andps	%xmm15, %xmm5
	addsd	%xmm5,  %xmm1
	addq	INCX, X

	andps	%xmm15, %xmm6
	addsd	%xmm6,  %xmm2
	andps	%xmm15, %xmm7
	addsd	%xmm7,  %xmm3
	ALIGN_3

.L26:
	testq	$2, M
	je	.L27

	movsd	(X), %xmm4
	addq	INCX, X
	movsd	(X), %xmm5
	addq	INCX, X

	andps	%xmm15, %xmm4
	andps	%xmm15, %xmm5

	addsd	%xmm4, %xmm0
	addsd	%xmm5, %xmm1
	ALIGN_3

.L27:
	testq	$1, M
	je	.L998

	movsd	(X), %xmm4
	andps	%xmm15, %xmm4
	addsd	%xmm4, %xmm0
	ALIGN_3

.L998:
	addsd	%xmm1, %xmm0
	addsd	%xmm3, %xmm2
	addsd	%xmm2, %xmm0
	ALIGN_4

.L999:
	RESTOREREGISTERS

	ret

	EPILOGUE

