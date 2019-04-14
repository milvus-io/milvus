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

#define N	ARG1	/* rdi */
#define X	ARG2	/* rsi */
#define INCX	ARG3	/* rdx */
#define Y	ARG4	/* rcx */
#ifndef WINDOWS_ABI
#define INCY	ARG5	/* r8  */
#else
#define INCY	%r10
#endif

#include "l1param.h"

	PROLOGUE
	PROFCODE

#ifdef WINDOWS_ABI
	movq	40(%rsp), INCY
#endif

	SAVEREGISTERS

	salq	$ZBASE_SHIFT, INCX
	salq	$ZBASE_SHIFT, INCY

	xorps	%xmm0, %xmm0
	xorps	%xmm1, %xmm1
	xorps	%xmm2, %xmm2
	xorps	%xmm3, %xmm3

	testq	N, N
	jle	.L999

	cmpq	$2 * SIZE, INCX
	jne	.L200
	cmpq	$2 * SIZE, INCY
	jne	.L200

	subq	$-32 * SIZE, X
	subq	$-32 * SIZE, Y

	testq	$SIZE, X
	jne	.L50

.L0x:
	testq	$2 * SIZE, X
	je	.L10

#ifdef movsd
	xorps	%xmm4, %xmm4
#endif
	movsd	-32 * SIZE(X), %xmm4
	movsd	-32 * SIZE(Y), %xmm0

	pshufd	$0xb1,  %xmm0, %xmm1
	mulps	%xmm4,  %xmm0
	mulps	%xmm4,  %xmm1
	addq	$2 * SIZE, X
	addq	$2 * SIZE, Y
	decq	N
	ALIGN_3

.L10:
	testq	$3 * SIZE, Y
	jne	.L20

	movq	N,  %rax
	sarq	$4, %rax
	jle	.L15

	movaps	-32 * SIZE(X), %xmm4
	movaps	-28 * SIZE(X), %xmm5
	movaps	-32 * SIZE(Y), %xmm8
	movaps	-28 * SIZE(Y), %xmm9
	movaps	-24 * SIZE(X), %xmm6
	movaps	-20 * SIZE(X), %xmm7
	movaps	-24 * SIZE(Y), %xmm10
	movaps	-20 * SIZE(Y), %xmm11

	decq	%rax
	jle	.L12
	ALIGN_3

.L11:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	pshufd	$0xb1,  %xmm8, %xmm12
	mulps	%xmm4,  %xmm8
	addps	%xmm8,  %xmm0
	movaps	-16 * SIZE(Y), %xmm8
	mulps	%xmm4,  %xmm12
	movaps	-16 * SIZE(X), %xmm4
	addps	%xmm12, %xmm1

	pshufd	$0xb1,  %xmm9, %xmm12
	mulps	%xmm5,  %xmm9
	addps	%xmm9,  %xmm2
	movaps	-12 * SIZE(Y), %xmm9
	mulps	%xmm5,  %xmm12
	movaps	-12 * SIZE(X), %xmm5
	addps	%xmm12, %xmm3

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	pshufd	$0xb1,  %xmm10, %xmm12
	mulps	%xmm6,  %xmm10
	addps	%xmm10, %xmm0
	movaps	 -8 * SIZE(Y), %xmm10
	mulps	%xmm6,  %xmm12
	movaps	 -8 * SIZE(X), %xmm6
	addps	%xmm12, %xmm1

	pshufd	$0xb1,  %xmm11, %xmm12
	mulps	%xmm7,  %xmm11
	addps	%xmm11, %xmm2
	movaps	 -4 * SIZE(Y), %xmm11
	mulps	%xmm7,  %xmm12
	movaps	 -4 * SIZE(X), %xmm7
	addps	%xmm12, %xmm3

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	pshufd	$0xb1,  %xmm8, %xmm12
	mulps	%xmm4,  %xmm8
	addps	%xmm8,  %xmm0
	movaps	  0 * SIZE(Y), %xmm8
	mulps	%xmm4,  %xmm12
	movaps	  0 * SIZE(X), %xmm4
	addps	%xmm12, %xmm1

	pshufd	$0xb1,  %xmm9, %xmm12
	mulps	%xmm5,  %xmm9
	addps	%xmm9,  %xmm2
	movaps	  4 * SIZE(Y), %xmm9
	mulps	%xmm5,  %xmm12
	movaps	  4 * SIZE(X), %xmm5
	addps	%xmm12, %xmm3

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	pshufd	$0xb1,  %xmm10, %xmm12
	mulps	%xmm6,  %xmm10
	addps	%xmm10, %xmm0
	movaps	  8 * SIZE(Y), %xmm10
	mulps	%xmm6,  %xmm12
	movaps	  8 * SIZE(X), %xmm6
	addps	%xmm12, %xmm1

	pshufd	$0xb1,  %xmm11, %xmm12
	mulps	%xmm7,  %xmm11
	addps	%xmm11, %xmm2
	movaps	 12 * SIZE(Y), %xmm11
	mulps	%xmm7,  %xmm12
	movaps	 12 * SIZE(X), %xmm7
	addps	%xmm12, %xmm3

	subq	$-32 * SIZE, X
	subq	$-32 * SIZE, Y

	decq	%rax
	jg	.L11
	ALIGN_3

.L12:
	pshufd	$0xb1,  %xmm8, %xmm12
	mulps	%xmm4,  %xmm8
	addps	%xmm8,  %xmm0
	movaps	-16 * SIZE(Y), %xmm8
	mulps	%xmm4,  %xmm12
	movaps	-16 * SIZE(X), %xmm4
	addps	%xmm12, %xmm1

	pshufd	$0xb1,  %xmm9, %xmm12
	mulps	%xmm5,  %xmm9
	addps	%xmm9,  %xmm2
	movaps	-12 * SIZE(Y), %xmm9
	mulps	%xmm5,  %xmm12
	movaps	-12 * SIZE(X), %xmm5
	addps	%xmm12, %xmm3

	pshufd	$0xb1,  %xmm10, %xmm12
	mulps	%xmm6,  %xmm10
	addps	%xmm10, %xmm0
	movaps	 -8 * SIZE(Y), %xmm10
	mulps	%xmm6,  %xmm12
	movaps	 -8 * SIZE(X), %xmm6
	addps	%xmm12, %xmm1

	pshufd	$0xb1,  %xmm11, %xmm12
	mulps	%xmm7,  %xmm11
	addps	%xmm11, %xmm2
	movaps	 -4 * SIZE(Y), %xmm11
	mulps	%xmm7,  %xmm12
	movaps	 -4 * SIZE(X), %xmm7
	addps	%xmm12, %xmm3

	pshufd	$0xb1,  %xmm8, %xmm12
	mulps	%xmm4,  %xmm8
	addps	%xmm8,  %xmm0
	mulps	%xmm4,  %xmm12
	addps	%xmm12, %xmm1

	pshufd	$0xb1,  %xmm9, %xmm12
	mulps	%xmm5,  %xmm9
	addps	%xmm9,  %xmm2
	mulps	%xmm5,  %xmm12
	addps	%xmm12, %xmm3

	pshufd	$0xb1,  %xmm10, %xmm12
	mulps	%xmm6,  %xmm10
	addps	%xmm10, %xmm0
	mulps	%xmm6,  %xmm12
	addps	%xmm12, %xmm1

	pshufd	$0xb1,  %xmm11, %xmm12
	mulps	%xmm7,  %xmm11
	addps	%xmm11, %xmm2
	mulps	%xmm7,  %xmm12
	addps	%xmm12, %xmm3

	subq	$-32 * SIZE, X
	subq	$-32 * SIZE, Y
	ALIGN_3

.L15:
	testq	$8, N
	jle	.L16

	movaps	-32 * SIZE(X), %xmm4
	movaps	-32 * SIZE(Y), %xmm8

	pshufd	$0xb1,  %xmm8, %xmm12
	mulps	%xmm4,  %xmm8
	addps	%xmm8,  %xmm0
	mulps	%xmm4,  %xmm12
	addps	%xmm12, %xmm1

	movaps	-28 * SIZE(X), %xmm5
	movaps	-28 * SIZE(Y), %xmm9

	pshufd	$0xb1,  %xmm9, %xmm12
	mulps	%xmm5,  %xmm9
	addps	%xmm9,  %xmm2
	mulps	%xmm5,  %xmm12
	addps	%xmm12, %xmm3

	movaps	-24 * SIZE(X), %xmm6
	movaps	-24 * SIZE(Y), %xmm10

	pshufd	$0xb1,  %xmm10, %xmm12
	mulps	%xmm6,  %xmm10
	addps	%xmm10, %xmm0
	mulps	%xmm6,  %xmm12
	addps	%xmm12, %xmm1

	movaps	-20 * SIZE(X), %xmm7
	movaps	-20 * SIZE(Y), %xmm11

	pshufd	$0xb1,  %xmm11, %xmm12
	mulps	%xmm7,  %xmm11
	addps	%xmm11, %xmm2
	mulps	%xmm7,  %xmm12
	addps	%xmm12, %xmm3

	addq	$16 * SIZE, X
	addq	$16 * SIZE, Y
	ALIGN_3

.L16:
	testq	$4, N
	jle	.L17

	movaps	-32 * SIZE(X), %xmm4
	movaps	-32 * SIZE(Y), %xmm8
	movaps	-28 * SIZE(X), %xmm5
	movaps	-28 * SIZE(Y), %xmm9

	pshufd	$0xb1,  %xmm8, %xmm12
	mulps	%xmm4,  %xmm8
	addps	%xmm8,  %xmm0
	mulps	%xmm4,  %xmm12
	addps	%xmm12, %xmm1

	pshufd	$0xb1,  %xmm9, %xmm12
	mulps	%xmm5,  %xmm9
	addps	%xmm9,  %xmm2
	mulps	%xmm5,  %xmm12
	addps	%xmm12, %xmm3

	addq	$8 * SIZE, X
	addq	$8 * SIZE, Y
	ALIGN_3

.L17:
	testq	$2, N
	jle	.L18

	movaps	-32 * SIZE(X), %xmm4
	movaps	-32 * SIZE(Y), %xmm8

	pshufd	$0xb1,  %xmm8, %xmm12
	mulps	%xmm4,  %xmm8
	addps	%xmm8,  %xmm0
	mulps	%xmm4,  %xmm12
	addps	%xmm12, %xmm1

	addq	$4 * SIZE, X
	addq	$4 * SIZE, Y
	ALIGN_3

.L18:
	testq	$1, N
	jle	.L98

#ifdef movsd
	xorps	%xmm4, %xmm4
#endif
	movsd	-32 * SIZE(X), %xmm4
#ifdef movsd
	xorps	%xmm8, %xmm8
#endif
	movsd	-32 * SIZE(Y), %xmm8

	pshufd	$0xb1,  %xmm8, %xmm12
	mulps	%xmm4,  %xmm8
	addps	%xmm8,  %xmm0
	mulps	%xmm4,  %xmm12
	addps	%xmm12, %xmm1
	jmp	.L98
	ALIGN_3

.L20:
#ifdef ALIGNED_ACCESS

	testq	$2 * SIZE, Y
	jne	.L30

	movaps	-33 * SIZE(Y), %xmm8
	addq	$3 * SIZE, Y

	shufps	$0xb1, %xmm1, %xmm1

	movq	N,  %rax
	sarq	$4, %rax
	jle	.L25

	movaps	-32 * SIZE(X), %xmm4
	movaps	-32 * SIZE(Y), %xmm9
	movaps	-28 * SIZE(X), %xmm5
	movaps	-28 * SIZE(Y), %xmm10
	movaps	-24 * SIZE(X), %xmm6
	movaps	-24 * SIZE(Y), %xmm11
	movaps	-20 * SIZE(X), %xmm7

	decq	%rax
	jle	.L22
	ALIGN_3

.L21:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	movss	%xmm9,  %xmm8
	pshufd	$0xb1,  %xmm4, %xmm12
	shufps	$0x39,  %xmm8, %xmm8
	mulps	%xmm8,  %xmm4
	addps	%xmm4,  %xmm0
	movaps	-16 * SIZE(X), %xmm4
	mulps	%xmm8,  %xmm12
	movaps	-20 * SIZE(Y), %xmm8
	addps	%xmm12, %xmm1

	movss	%xmm10, %xmm9
	pshufd	$0xb1,  %xmm5, %xmm12
	shufps	$0x39,  %xmm9, %xmm9
	mulps	%xmm9,  %xmm5
	addps	%xmm5,  %xmm0
	movaps	-12 * SIZE(X), %xmm5
	mulps	%xmm9,  %xmm12
	movaps	-16 * SIZE(Y), %xmm9
	addps	%xmm12, %xmm1

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movss	%xmm11, %xmm10
	pshufd	$0xb1,  %xmm6,  %xmm12
	shufps	$0x39,  %xmm10, %xmm10
	mulps	%xmm10, %xmm6
	addps	%xmm6,  %xmm0
	movaps	 -8 * SIZE(X), %xmm6
	mulps	%xmm10, %xmm12
	movaps	-12 * SIZE(Y), %xmm10
	addps	%xmm12, %xmm1

	movss	%xmm8,  %xmm11
	pshufd	$0xb1,  %xmm7,  %xmm12
	shufps	$0x39,  %xmm11, %xmm11
	mulps	%xmm11, %xmm7
	addps	%xmm7,  %xmm0
	movaps	 -4 * SIZE(X), %xmm7
	mulps	%xmm11, %xmm12
	movaps	 -8 * SIZE(Y), %xmm11
	addps	%xmm12, %xmm1

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	movss	%xmm9,  %xmm8
	pshufd	$0xb1,  %xmm4, %xmm12
	shufps	$0x39,  %xmm8, %xmm8
	mulps	%xmm8,  %xmm4
	addps	%xmm4,  %xmm0
	movaps	  0 * SIZE(X), %xmm4
	mulps	%xmm8,  %xmm12
	movaps	 -4 * SIZE(Y), %xmm8
	addps	%xmm12, %xmm1

	movss	%xmm10, %xmm9
	pshufd	$0xb1,  %xmm5, %xmm12
	shufps	$0x39,  %xmm9, %xmm9
	mulps	%xmm9,  %xmm5
	addps	%xmm5,  %xmm0
	movaps	  4 * SIZE(X), %xmm5
	mulps	%xmm9,  %xmm12
	movaps	  0 * SIZE(Y), %xmm9
	addps	%xmm12, %xmm1

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	movss	%xmm11, %xmm10
	pshufd	$0xb1,  %xmm6,  %xmm12
	shufps	$0x39,  %xmm10, %xmm10
	mulps	%xmm10, %xmm6
	addps	%xmm6,  %xmm0
	movaps	  8 * SIZE(X), %xmm6
	mulps	%xmm10, %xmm12
	movaps	  4 * SIZE(Y), %xmm10
	addps	%xmm12, %xmm1

	movss	%xmm8,  %xmm11
	pshufd	$0xb1,  %xmm7,  %xmm12
	shufps	$0x39,  %xmm11, %xmm11
	mulps	%xmm11, %xmm7
	addps	%xmm7,  %xmm0
	movaps	 12 * SIZE(X), %xmm7
	mulps	%xmm11, %xmm12
	movaps	  8 * SIZE(Y), %xmm11
	addps	%xmm12, %xmm1

	subq	$-32 * SIZE, X
	subq	$-32 * SIZE, Y

	decq	%rax
	jg	.L21
	ALIGN_3

.L22:
	movss	%xmm9,  %xmm8
	pshufd	$0xb1,  %xmm4, %xmm12
	shufps	$0x39,  %xmm8, %xmm8
	mulps	%xmm8,  %xmm4
	addps	%xmm4,  %xmm0
	movaps	-16 * SIZE(X), %xmm4
	mulps	%xmm8,  %xmm12
	movaps	-20 * SIZE(Y), %xmm8
	addps	%xmm12, %xmm1

	movss	%xmm10, %xmm9
	pshufd	$0xb1,  %xmm5, %xmm12
	shufps	$0x39,  %xmm9, %xmm9
	mulps	%xmm9,  %xmm5
	addps	%xmm5,  %xmm0
	movaps	-12 * SIZE(X), %xmm5
	mulps	%xmm9,  %xmm12
	movaps	-16 * SIZE(Y), %xmm9
	addps	%xmm12, %xmm1

	movss	%xmm11, %xmm10
	pshufd	$0xb1,  %xmm6,  %xmm12
	shufps	$0x39,  %xmm10, %xmm10
	mulps	%xmm10, %xmm6
	addps	%xmm6,  %xmm0
	movaps	 -8 * SIZE(X), %xmm6
	mulps	%xmm10, %xmm12
	movaps	-12 * SIZE(Y), %xmm10
	addps	%xmm12, %xmm1

	movss	%xmm8,  %xmm11
	pshufd	$0xb1,  %xmm7,  %xmm12
	shufps	$0x39,  %xmm11, %xmm11
	mulps	%xmm11, %xmm7
	addps	%xmm7,  %xmm0
	movaps	 -4 * SIZE(X), %xmm7
	mulps	%xmm11, %xmm12
	movaps	 -8 * SIZE(Y), %xmm11
	addps	%xmm12, %xmm1

	movss	%xmm9,  %xmm8
	pshufd	$0xb1,  %xmm4, %xmm12
	shufps	$0x39,  %xmm8, %xmm8
	mulps	%xmm8,  %xmm4
	addps	%xmm4,  %xmm0
	mulps	%xmm8,  %xmm12
	movaps	 -4 * SIZE(Y), %xmm8
	addps	%xmm12, %xmm1

	movss	%xmm10, %xmm9
	pshufd	$0xb1,  %xmm5, %xmm12
	shufps	$0x39,  %xmm9, %xmm9
	mulps	%xmm9,  %xmm5
	addps	%xmm5,  %xmm0
	mulps	%xmm9,  %xmm12
	addps	%xmm12, %xmm1

	movss	%xmm11, %xmm10
	pshufd	$0xb1,  %xmm6,  %xmm12
	shufps	$0x39,  %xmm10, %xmm10
	mulps	%xmm10, %xmm6
	addps	%xmm6,  %xmm0
	mulps	%xmm10, %xmm12
	addps	%xmm12, %xmm1

	movss	%xmm8,  %xmm11
	pshufd	$0xb1,  %xmm7,  %xmm12
	shufps	$0x39,  %xmm11, %xmm11
	mulps	%xmm11, %xmm7
	addps	%xmm7,  %xmm0
	mulps	%xmm11, %xmm12
	addps	%xmm12, %xmm1

	subq	$-32 * SIZE, X
	subq	$-32 * SIZE, Y
	ALIGN_3

.L25:
	testq	$8, N
	jle	.L26

	movaps	-32 * SIZE(X), %xmm4
	movaps	-32 * SIZE(Y), %xmm9
	movaps	-28 * SIZE(X), %xmm5
	movaps	-28 * SIZE(Y), %xmm10

	movss	%xmm9,  %xmm8
	pshufd	$0xb1,  %xmm4, %xmm12
	shufps	$0x39,  %xmm8, %xmm8
	mulps	%xmm8,  %xmm4
	addps	%xmm4,  %xmm0
	mulps	%xmm8,  %xmm12
	addps	%xmm12, %xmm1

	movaps	-24 * SIZE(X), %xmm6
	movaps	-24 * SIZE(Y), %xmm11

	movss	%xmm10, %xmm9
	pshufd	$0xb1,  %xmm5, %xmm12
	shufps	$0x39,  %xmm9, %xmm9
	mulps	%xmm9,  %xmm5
	addps	%xmm5,  %xmm0
	mulps	%xmm9,  %xmm12
	addps	%xmm12, %xmm1

	movaps	-20 * SIZE(X), %xmm7
	movaps	-20 * SIZE(Y), %xmm8

	movss	%xmm11, %xmm10
	pshufd	$0xb1,  %xmm6,  %xmm12
	shufps	$0x39,  %xmm10, %xmm10
	mulps	%xmm10, %xmm6
	addps	%xmm6,  %xmm0
	mulps	%xmm10, %xmm12
	addps	%xmm12, %xmm1

	movss	%xmm8,  %xmm11
	pshufd	$0xb1,  %xmm7,  %xmm12
	shufps	$0x39,  %xmm11, %xmm11
	mulps	%xmm11, %xmm7
	addps	%xmm7,  %xmm0
	mulps	%xmm11, %xmm12
	addps	%xmm12, %xmm1

	addq	$16 * SIZE, X
	addq	$16 * SIZE, Y
	ALIGN_3

.L26:
	testq	$4, N
	jle	.L27

	movaps	-32 * SIZE(X), %xmm4
	movaps	-32 * SIZE(Y), %xmm9

	movss	%xmm9,  %xmm8
	pshufd	$0xb1,  %xmm4, %xmm12
	shufps	$0x39,  %xmm8, %xmm8
	mulps	%xmm8,  %xmm4
	addps	%xmm4,  %xmm0
	mulps	%xmm8,  %xmm12
	addps	%xmm12, %xmm1

	movaps	-28 * SIZE(X), %xmm5
	movaps	-28 * SIZE(Y), %xmm10

	movss	%xmm10, %xmm9
	pshufd	$0xb1,  %xmm5, %xmm12
	shufps	$0x39,  %xmm9, %xmm9
	mulps	%xmm9,  %xmm5
	addps	%xmm5,  %xmm0
	mulps	%xmm9,  %xmm12
	addps	%xmm12, %xmm1

	movaps	%xmm10, %xmm8

	addq	$8 * SIZE, X
	addq	$8 * SIZE, Y
	ALIGN_3

.L27:
	testq	$2, N
	jle	.L28

	movaps	-32 * SIZE(X), %xmm4
	movaps	-32 * SIZE(Y), %xmm9

	movss	%xmm9,  %xmm8
	pshufd	$0xb1,  %xmm4, %xmm12
	shufps	$0x39,  %xmm8, %xmm8
	mulps	%xmm8,  %xmm4
	addps	%xmm4,  %xmm0
	mulps	%xmm8,  %xmm12
	addps	%xmm12, %xmm1

	movaps	%xmm9, %xmm8
	addq	$4 * SIZE, X
	addq	$4 * SIZE, Y
	ALIGN_3

.L28:
	testq	$1, N
	jle	.L29

#ifdef movsd
	xorps	%xmm4, %xmm4
#endif
	movsd	-32 * SIZE(X), %xmm4

	pshufd	$0xb1,  %xmm4, %xmm12
	shufps	$0x59,  %xmm8, %xmm8
	mulps	%xmm8,  %xmm4
	addps	%xmm4,  %xmm0
	mulps	%xmm8,  %xmm12
	addps	%xmm12, %xmm1
	ALIGN_3

.L29:
	shufps	$0xb1, %xmm1, %xmm1
	shufps	$0xb1, %xmm3, %xmm3
	jmp	.L98
	ALIGN_3

.L30:

	testq	$SIZE, Y
	jne	.L40
#endif

	movq	N,  %rax
	sarq	$4, %rax
	jle	.L35

	movaps	-32 * SIZE(X), %xmm4
	movsd	-32 * SIZE(Y), %xmm8
	movhps	-30 * SIZE(Y), %xmm8
	movaps	-28 * SIZE(X), %xmm5
	movsd	-28 * SIZE(Y), %xmm9
	movhps	-26 * SIZE(Y), %xmm9

	movaps	-24 * SIZE(X), %xmm6
	movsd	-24 * SIZE(Y), %xmm10
	movhps	-22 * SIZE(Y), %xmm10
	movaps	-20 * SIZE(X), %xmm7
	movsd	-20 * SIZE(Y), %xmm11
	movhps	-18 * SIZE(Y), %xmm11

	decq	%rax
	jle	.L32
	ALIGN_3

.L31:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	pshufd	$0xb1,  %xmm8, %xmm12
	mulps	%xmm4,  %xmm8
	addps	%xmm8,  %xmm0
	movsd	-16 * SIZE(Y), %xmm8
	movhps	-14 * SIZE(Y), %xmm8
	mulps	%xmm4,  %xmm12
	movaps	-16 * SIZE(X), %xmm4
	addps	%xmm12, %xmm1

	pshufd	$0xb1,  %xmm9, %xmm12
	mulps	%xmm5,  %xmm9
	addps	%xmm9,  %xmm2
	movsd	-12 * SIZE(Y), %xmm9
	movhps	-10 * SIZE(Y), %xmm9
	mulps	%xmm5,  %xmm12
	movaps	-12 * SIZE(X), %xmm5
	addps	%xmm12, %xmm3

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	pshufd	$0xb1,  %xmm10, %xmm12
	mulps	%xmm6,  %xmm10
	addps	%xmm10, %xmm0
	movsd	 -8 * SIZE(Y), %xmm10
	movhps	 -6 * SIZE(Y), %xmm10
	mulps	%xmm6,  %xmm12
	movaps	 -8 * SIZE(X), %xmm6
	addps	%xmm12, %xmm1

	pshufd	$0xb1,  %xmm11, %xmm12
	mulps	%xmm7,  %xmm11
	addps	%xmm11, %xmm2
	movsd	 -4 * SIZE(Y), %xmm11
	movhps	 -2 * SIZE(Y), %xmm11
	mulps	%xmm7,  %xmm12
	movaps	 -4 * SIZE(X), %xmm7
	addps	%xmm12, %xmm3

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	pshufd	$0xb1,  %xmm8, %xmm12
	mulps	%xmm4,  %xmm8
	addps	%xmm8,  %xmm0
	movsd	  0 * SIZE(Y), %xmm8
	movhps	  2 * SIZE(Y), %xmm8
	mulps	%xmm4,  %xmm12
	movaps	  0 * SIZE(X), %xmm4
	addps	%xmm12, %xmm1

	pshufd	$0xb1,  %xmm9, %xmm12
	mulps	%xmm5,  %xmm9
	addps	%xmm9,  %xmm2
	movsd	  4 * SIZE(Y), %xmm9
	movhps	  6 * SIZE(Y), %xmm9
	mulps	%xmm5,  %xmm12
	movaps	  4 * SIZE(X), %xmm5
	addps	%xmm12, %xmm3

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	pshufd	$0xb1,  %xmm10, %xmm12
	mulps	%xmm6,  %xmm10
	addps	%xmm10, %xmm0
	movsd	  8 * SIZE(Y), %xmm10
	movhps	 10 * SIZE(Y), %xmm10
	mulps	%xmm6,  %xmm12
	movaps	  8 * SIZE(X), %xmm6
	addps	%xmm12, %xmm1

	pshufd	$0xb1,  %xmm11, %xmm12
	mulps	%xmm7,  %xmm11
	addps	%xmm11, %xmm2
	movsd	 12 * SIZE(Y), %xmm11
	movhps	 14 * SIZE(Y), %xmm11
	mulps	%xmm7,  %xmm12
	movaps	 12 * SIZE(X), %xmm7
	addps	%xmm12, %xmm3

	subq	$-32 * SIZE, X
	subq	$-32 * SIZE, Y

	decq	%rax
	jg	.L31
	ALIGN_3

.L32:
	pshufd	$0xb1,  %xmm8, %xmm12
	mulps	%xmm4,  %xmm8
	addps	%xmm8,  %xmm0
	movsd	-16 * SIZE(Y), %xmm8
	movhps	-14 * SIZE(Y), %xmm8
	mulps	%xmm4,  %xmm12
	movaps	-16 * SIZE(X), %xmm4
	addps	%xmm12, %xmm1

	pshufd	$0xb1,  %xmm9, %xmm12
	mulps	%xmm5,  %xmm9
	addps	%xmm9,  %xmm2
	movsd	-12 * SIZE(Y), %xmm9
	movhps	-10 * SIZE(Y), %xmm9
	mulps	%xmm5,  %xmm12
	movaps	-12 * SIZE(X), %xmm5
	addps	%xmm12, %xmm3

	pshufd	$0xb1,  %xmm10, %xmm12
	mulps	%xmm6,  %xmm10
	addps	%xmm10, %xmm0
	movsd	 -8 * SIZE(Y), %xmm10
	movhps	 -6 * SIZE(Y), %xmm10
	mulps	%xmm6,  %xmm12
	movaps	 -8 * SIZE(X), %xmm6
	addps	%xmm12, %xmm1

	pshufd	$0xb1,  %xmm11, %xmm12
	mulps	%xmm7,  %xmm11
	addps	%xmm11, %xmm2
	movsd	 -4 * SIZE(Y), %xmm11
	movhps	 -2 * SIZE(Y), %xmm11
	mulps	%xmm7,  %xmm12
	movaps	 -4 * SIZE(X), %xmm7
	addps	%xmm12, %xmm3

	pshufd	$0xb1,  %xmm8, %xmm12
	mulps	%xmm4,  %xmm8
	addps	%xmm8,  %xmm0
	mulps	%xmm4,  %xmm12
	addps	%xmm12, %xmm1

	pshufd	$0xb1,  %xmm9, %xmm12
	mulps	%xmm5,  %xmm9
	addps	%xmm9,  %xmm2
	mulps	%xmm5,  %xmm12
	addps	%xmm12, %xmm3

	pshufd	$0xb1,  %xmm10, %xmm12
	mulps	%xmm6,  %xmm10
	addps	%xmm10, %xmm0
	mulps	%xmm6,  %xmm12
	addps	%xmm12, %xmm1

	pshufd	$0xb1,  %xmm11, %xmm12
	mulps	%xmm7,  %xmm11
	addps	%xmm11, %xmm2
	mulps	%xmm7,  %xmm12
	addps	%xmm12, %xmm3

	subq	$-32 * SIZE, X
	subq	$-32 * SIZE, Y
	ALIGN_3

.L35:
	testq	$8, N
	jle	.L36

	movaps	-32 * SIZE(X), %xmm4
	movsd	-32 * SIZE(Y), %xmm8
	movhps	-30 * SIZE(Y), %xmm8

	pshufd	$0xb1,  %xmm8, %xmm12
	mulps	%xmm4,  %xmm8
	addps	%xmm8,  %xmm0
	mulps	%xmm4,  %xmm12
	addps	%xmm12, %xmm1

	movaps	-28 * SIZE(X), %xmm5
	movsd	-28 * SIZE(Y), %xmm9
	movhps	-26 * SIZE(Y), %xmm9

	pshufd	$0xb1,  %xmm9, %xmm12
	mulps	%xmm5,  %xmm9
	addps	%xmm9,  %xmm2
	mulps	%xmm5,  %xmm12
	addps	%xmm12, %xmm3

	movaps	-24 * SIZE(X), %xmm6
	movsd	-24 * SIZE(Y), %xmm10
	movhps	-22 * SIZE(Y), %xmm10

	pshufd	$0xb1,  %xmm10, %xmm12
	mulps	%xmm6,  %xmm10
	addps	%xmm10, %xmm0
	mulps	%xmm6,  %xmm12
	addps	%xmm12, %xmm1

	movaps	-20 * SIZE(X), %xmm7
	movsd	-20 * SIZE(Y), %xmm11
	movhps	-18 * SIZE(Y), %xmm11

	pshufd	$0xb1,  %xmm11, %xmm12
	mulps	%xmm7,  %xmm11
	addps	%xmm11, %xmm2
	mulps	%xmm7,  %xmm12
	addps	%xmm12, %xmm3

	addq	$16 * SIZE, X
	addq	$16 * SIZE, Y
	ALIGN_3

.L36:
	testq	$4, N
	jle	.L37

	movaps	-32 * SIZE(X), %xmm4
	movsd	-32 * SIZE(Y), %xmm8
	movhps	-30 * SIZE(Y), %xmm8

	pshufd	$0xb1,  %xmm8, %xmm12
	mulps	%xmm4,  %xmm8
	addps	%xmm8,  %xmm0
	mulps	%xmm4,  %xmm12
	addps	%xmm12, %xmm1

	movaps	-28 * SIZE(X), %xmm5
	movsd	-28 * SIZE(Y), %xmm9
	movhps	-26 * SIZE(Y), %xmm9

	pshufd	$0xb1,  %xmm9, %xmm12
	mulps	%xmm5,  %xmm9
	addps	%xmm9,  %xmm2
	mulps	%xmm5,  %xmm12
	addps	%xmm12, %xmm3

	addq	$8 * SIZE, X
	addq	$8 * SIZE, Y
	ALIGN_3

.L37:
	testq	$2, N
	jle	.L38

	movaps	-32 * SIZE(X), %xmm4
	movsd	-32 * SIZE(Y), %xmm8
	movhps	-30 * SIZE(Y), %xmm8

	pshufd	$0xb1,  %xmm8, %xmm12
	mulps	%xmm4,  %xmm8
	addps	%xmm8,  %xmm0
	mulps	%xmm4,  %xmm12
	addps	%xmm12, %xmm1

	addq	$4 * SIZE, X
	addq	$4 * SIZE, Y
	ALIGN_3

.L38:
	testq	$1, N
	jle	.L98

#ifdef movsd
	xorps	%xmm4, %xmm4
#endif
	movsd	-32 * SIZE(X), %xmm4
#ifdef movsd
	xorps	%xmm8, %xmm8
#endif
	movsd	-32 * SIZE(Y), %xmm8

	pshufd	$0xb1,  %xmm8, %xmm12
	mulps	%xmm4,  %xmm8
	addps	%xmm8,  %xmm0
	mulps	%xmm4,  %xmm12
	addps	%xmm12, %xmm1
	jmp	.L98
	ALIGN_3

#ifdef ALIGNED_ACCESS
.L40:
	movaps	-35 * SIZE(Y), %xmm8
	addq	$1 * SIZE, Y

	shufps	$0xb1, %xmm1, %xmm1

	movq	N,  %rax
	sarq	$4, %rax
	jle	.L45

	movaps	-32 * SIZE(X), %xmm4
	movaps	-32 * SIZE(Y), %xmm9
	movaps	-28 * SIZE(X), %xmm5
	movaps	-28 * SIZE(Y), %xmm10
	movaps	-24 * SIZE(X), %xmm6
	movaps	-24 * SIZE(Y), %xmm11
	movaps	-20 * SIZE(X), %xmm7

	decq	%rax
	jle	.L42
	ALIGN_3

.L41:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	movss	%xmm9,  %xmm8
	pshufd	$0xb1,  %xmm4, %xmm12
	shufps	$0x93,  %xmm9, %xmm8
	mulps	%xmm8,  %xmm4
	addps	%xmm4,  %xmm0
	movaps	-16 * SIZE(X), %xmm4
	mulps	%xmm8,  %xmm12
	movaps	-20 * SIZE(Y), %xmm8
	addps	%xmm12, %xmm1

	movss	%xmm10, %xmm9
	pshufd	$0xb1,  %xmm5, %xmm12
	shufps	$0x93,  %xmm10, %xmm9
	mulps	%xmm9,  %xmm5
	addps	%xmm5,  %xmm0
	movaps	-12 * SIZE(X), %xmm5
	mulps	%xmm9,  %xmm12
	movaps	-16 * SIZE(Y), %xmm9
	addps	%xmm12, %xmm1

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movss	%xmm11, %xmm10
	pshufd	$0xb1,  %xmm6,  %xmm12
	shufps	$0x93,  %xmm11, %xmm10
	mulps	%xmm10, %xmm6
	addps	%xmm6,  %xmm0
	movaps	 -8 * SIZE(X), %xmm6
	mulps	%xmm10, %xmm12
	movaps	-12 * SIZE(Y), %xmm10
	addps	%xmm12, %xmm1

	movss	%xmm8,  %xmm11
	pshufd	$0xb1,  %xmm7,  %xmm12
	shufps	$0x93,  %xmm8, %xmm11
	mulps	%xmm11, %xmm7
	addps	%xmm7,  %xmm0
	movaps	 -4 * SIZE(X), %xmm7
	mulps	%xmm11, %xmm12
	movaps	 -8 * SIZE(Y), %xmm11
	addps	%xmm12, %xmm1

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	movss	%xmm9,  %xmm8
	pshufd	$0xb1,  %xmm4, %xmm12
	shufps	$0x93,  %xmm9, %xmm8
	mulps	%xmm8,  %xmm4
	addps	%xmm4,  %xmm0
	movaps	  0 * SIZE(X), %xmm4
	mulps	%xmm8,  %xmm12
	movaps	 -4 * SIZE(Y), %xmm8
	addps	%xmm12, %xmm1

	movss	%xmm10, %xmm9
	pshufd	$0xb1,  %xmm5, %xmm12
	shufps	$0x93,  %xmm10, %xmm9
	mulps	%xmm9,  %xmm5
	addps	%xmm5,  %xmm0
	movaps	  4 * SIZE(X), %xmm5
	mulps	%xmm9,  %xmm12
	movaps	  0 * SIZE(Y), %xmm9
	addps	%xmm12, %xmm1

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	movss	%xmm11, %xmm10
	pshufd	$0xb1,  %xmm6,  %xmm12
	shufps	$0x93,  %xmm11, %xmm10
	mulps	%xmm10, %xmm6
	addps	%xmm6,  %xmm0
	movaps	  8 * SIZE(X), %xmm6
	mulps	%xmm10, %xmm12
	movaps	  4 * SIZE(Y), %xmm10
	addps	%xmm12, %xmm1

	movss	%xmm8,  %xmm11
	pshufd	$0xb1,  %xmm7,  %xmm12
	shufps	$0x93,  %xmm8, %xmm11
	mulps	%xmm11, %xmm7
	addps	%xmm7,  %xmm0
	movaps	 12 * SIZE(X), %xmm7
	mulps	%xmm11, %xmm12
	movaps	  8 * SIZE(Y), %xmm11
	addps	%xmm12, %xmm1

	subq	$-32 * SIZE, X
	subq	$-32 * SIZE, Y

	decq	%rax
	jg	.L41
	ALIGN_3

.L42:
	movss	%xmm9,  %xmm8
	pshufd	$0xb1,  %xmm4, %xmm12
	shufps	$0x93,  %xmm9, %xmm8
	mulps	%xmm8,  %xmm4
	addps	%xmm4,  %xmm0
	movaps	-16 * SIZE(X), %xmm4
	mulps	%xmm8,  %xmm12
	movaps	-20 * SIZE(Y), %xmm8
	addps	%xmm12, %xmm1

	movss	%xmm10, %xmm9
	pshufd	$0xb1,  %xmm5, %xmm12
	shufps	$0x93,  %xmm10, %xmm9
	mulps	%xmm9,  %xmm5
	addps	%xmm5,  %xmm0
	movaps	-12 * SIZE(X), %xmm5
	mulps	%xmm9,  %xmm12
	movaps	-16 * SIZE(Y), %xmm9
	addps	%xmm12, %xmm1

	movss	%xmm11, %xmm10
	pshufd	$0xb1,  %xmm6,  %xmm12
	shufps	$0x93,  %xmm11, %xmm10
	mulps	%xmm10, %xmm6
	addps	%xmm6,  %xmm0
	movaps	 -8 * SIZE(X), %xmm6
	mulps	%xmm10, %xmm12
	movaps	-12 * SIZE(Y), %xmm10
	addps	%xmm12, %xmm1

	movss	%xmm8,  %xmm11
	pshufd	$0xb1,  %xmm7,  %xmm12
	shufps	$0x93,  %xmm8, %xmm11
	mulps	%xmm11, %xmm7
	addps	%xmm7,  %xmm0
	movaps	 -4 * SIZE(X), %xmm7
	mulps	%xmm11, %xmm12
	movaps	 -8 * SIZE(Y), %xmm11
	addps	%xmm12, %xmm1

	movss	%xmm9,  %xmm8
	pshufd	$0xb1,  %xmm4, %xmm12
	shufps	$0x93,  %xmm9, %xmm8
	mulps	%xmm8,  %xmm4
	addps	%xmm4,  %xmm0
	mulps	%xmm8,  %xmm12
	movaps	 -4 * SIZE(Y), %xmm8
	addps	%xmm12, %xmm1

	movss	%xmm10, %xmm9
	pshufd	$0xb1,  %xmm5, %xmm12
	shufps	$0x93,  %xmm10, %xmm9
	mulps	%xmm9,  %xmm5
	addps	%xmm5,  %xmm0
	mulps	%xmm9,  %xmm12
	addps	%xmm12, %xmm1

	movss	%xmm11, %xmm10
	pshufd	$0xb1,  %xmm6,  %xmm12
	shufps	$0x93,  %xmm11, %xmm10
	mulps	%xmm10, %xmm6
	addps	%xmm6,  %xmm0
	mulps	%xmm10, %xmm12
	addps	%xmm12, %xmm1

	movss	%xmm8,  %xmm11
	pshufd	$0xb1,  %xmm7,  %xmm12
	shufps	$0x93,  %xmm8, %xmm11
	mulps	%xmm11, %xmm7
	addps	%xmm7,  %xmm0
	mulps	%xmm11, %xmm12
	addps	%xmm12, %xmm1

	subq	$-32 * SIZE, X
	subq	$-32 * SIZE, Y
	ALIGN_3

.L45:
	testq	$8, N
	jle	.L46

	movaps	-32 * SIZE(X), %xmm4
	movaps	-32 * SIZE(Y), %xmm9
	movaps	-28 * SIZE(X), %xmm5
	movaps	-28 * SIZE(Y), %xmm10

	movss	%xmm9,  %xmm8
	pshufd	$0xb1,  %xmm4, %xmm12
	shufps	$0x93,  %xmm9, %xmm8
	mulps	%xmm8,  %xmm4
	addps	%xmm4,  %xmm0
	mulps	%xmm8,  %xmm12
	addps	%xmm12, %xmm1

	movaps	-24 * SIZE(X), %xmm6
	movaps	-24 * SIZE(Y), %xmm11

	movss	%xmm10, %xmm9
	pshufd	$0xb1,  %xmm5, %xmm12
	shufps	$0x93,  %xmm10, %xmm9
	mulps	%xmm9,  %xmm5
	addps	%xmm5,  %xmm0
	mulps	%xmm9,  %xmm12
	addps	%xmm12, %xmm1

	movaps	-20 * SIZE(X), %xmm7
	movaps	-20 * SIZE(Y), %xmm8

	movss	%xmm11, %xmm10
	pshufd	$0xb1,  %xmm6,  %xmm12
	shufps	$0x93,  %xmm11, %xmm10
	mulps	%xmm10, %xmm6
	addps	%xmm6,  %xmm0
	mulps	%xmm10, %xmm12
	addps	%xmm12, %xmm1

	movss	%xmm8,  %xmm11
	pshufd	$0xb1,  %xmm7,  %xmm12
	shufps	$0x93,  %xmm8, %xmm11
	mulps	%xmm11, %xmm7
	addps	%xmm7,  %xmm0
	mulps	%xmm11, %xmm12
	addps	%xmm12, %xmm1

	addq	$16 * SIZE, X
	addq	$16 * SIZE, Y
	ALIGN_3

.L46:
	testq	$4, N
	jle	.L47

	movaps	-32 * SIZE(X), %xmm4
	movaps	-32 * SIZE(Y), %xmm9

	movss	%xmm9,  %xmm8
	pshufd	$0xb1,  %xmm4, %xmm12
	shufps	$0x93,  %xmm9, %xmm8
	mulps	%xmm8,  %xmm4
	addps	%xmm4,  %xmm0
	mulps	%xmm8,  %xmm12
	addps	%xmm12, %xmm1

	movaps	-28 * SIZE(X), %xmm5
	movaps	-28 * SIZE(Y), %xmm10

	movss	%xmm10, %xmm9
	pshufd	$0xb1,  %xmm5, %xmm12
	shufps	$0x93,  %xmm10, %xmm9
	mulps	%xmm9,  %xmm5
	addps	%xmm5,  %xmm0
	mulps	%xmm9,  %xmm12
	addps	%xmm12, %xmm1

	movaps	%xmm10, %xmm8

	addq	$8 * SIZE, X
	addq	$8 * SIZE, Y
	ALIGN_3

.L47:
	testq	$2, N
	jle	.L48

	movaps	-32 * SIZE(X), %xmm4
	movaps	-32 * SIZE(Y), %xmm9

	movss	%xmm9,  %xmm8
	pshufd	$0xb1,  %xmm4, %xmm12
	shufps	$0x93,  %xmm9, %xmm8
	mulps	%xmm8,  %xmm4
	addps	%xmm4,  %xmm0
	mulps	%xmm8,  %xmm12
	addps	%xmm12, %xmm1

	movaps	%xmm9, %xmm8
	addq	$4 * SIZE, X
	addq	$4 * SIZE, Y
	ALIGN_3

.L48:
	testq	$1, N
	jle	.L49

#ifdef movsd
	xorps	%xmm4, %xmm4
#endif
	movsd	-32 * SIZE(X), %xmm4
	movss	-32 * SIZE(Y), %xmm9

	movss	%xmm9,  %xmm8
	pshufd	$0xb1,  %xmm4, %xmm12
	shufps	$0x03,  %xmm8, %xmm8
	mulps	%xmm8,  %xmm4
	addps	%xmm4,  %xmm0
	mulps	%xmm8,  %xmm12
	addps	%xmm12, %xmm1
	ALIGN_3

.L49:
	shufps	$0xb1, %xmm1, %xmm1
	shufps	$0xb1, %xmm3, %xmm3
	jmp	.L98
	ALIGN_3
#endif

.L50:
	testq	$SIZE, Y
	jne	.L70

#ifdef ALIGNED_ACCESS

	testq	$2 * SIZE, Y
	je	.L50x

#ifdef movsd
	xorps	%xmm0, %xmm0
#endif
	movsd	-32 * SIZE(X), %xmm0
#ifdef movsd
	xorps	%xmm4, %xmm4
#endif
	movsd	-32 * SIZE(Y), %xmm4

	pshufd	$0xb1,  %xmm0, %xmm1
	mulps	%xmm4,  %xmm0
	mulps	%xmm4,  %xmm1
	addq	$2 * SIZE, X
	addq	$2 * SIZE, Y

	decq	N
	ALIGN_3

.L50x:
	testq	$2 * SIZE, X
	jne	.L60

	movaps	-33 * SIZE(X), %xmm8
	addq	$3 * SIZE, X

	shufps	$0xb1, %xmm1, %xmm1

	movq	N,  %rax
	sarq	$4, %rax
	jle	.L55

	movaps	-32 * SIZE(Y), %xmm4
	movaps	-32 * SIZE(X), %xmm9
	movaps	-28 * SIZE(Y), %xmm5
	movaps	-28 * SIZE(X), %xmm10
	movaps	-24 * SIZE(Y), %xmm6
	movaps	-24 * SIZE(X), %xmm11
	movaps	-20 * SIZE(Y), %xmm7

	decq	%rax
	jle	.L52
	ALIGN_3

.L51:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movss	%xmm9,  %xmm8
	pshufd	$0xb1,  %xmm4, %xmm12
	shufps	$0x39,  %xmm8, %xmm8
	mulps	%xmm8,  %xmm4
	addps	%xmm4,  %xmm0
	movaps	-16 * SIZE(Y), %xmm4
	mulps	%xmm8,  %xmm12
	movaps	-20 * SIZE(X), %xmm8
	addps	%xmm12, %xmm1

	movss	%xmm10, %xmm9
	pshufd	$0xb1,  %xmm5, %xmm12
	shufps	$0x39,  %xmm9, %xmm9
	mulps	%xmm9,  %xmm5
	addps	%xmm5,  %xmm0
	movaps	-12 * SIZE(Y), %xmm5
	mulps	%xmm9,  %xmm12
	movaps	-16 * SIZE(X), %xmm9
	addps	%xmm12, %xmm1

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	movss	%xmm11, %xmm10
	pshufd	$0xb1,  %xmm6,  %xmm12
	shufps	$0x39,  %xmm10, %xmm10
	mulps	%xmm10, %xmm6
	addps	%xmm6,  %xmm0
	movaps	 -8 * SIZE(Y), %xmm6
	mulps	%xmm10, %xmm12
	movaps	-12 * SIZE(X), %xmm10
	addps	%xmm12, %xmm1

	movss	%xmm8,  %xmm11
	pshufd	$0xb1,  %xmm7,  %xmm12
	shufps	$0x39,  %xmm11, %xmm11
	mulps	%xmm11, %xmm7
	addps	%xmm7,  %xmm0
	movaps	 -4 * SIZE(Y), %xmm7
	mulps	%xmm11, %xmm12
	movaps	 -8 * SIZE(X), %xmm11
	addps	%xmm12, %xmm1

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	movss	%xmm9,  %xmm8
	pshufd	$0xb1,  %xmm4, %xmm12
	shufps	$0x39,  %xmm8, %xmm8
	mulps	%xmm8,  %xmm4
	addps	%xmm4,  %xmm0
	movaps	  0 * SIZE(Y), %xmm4
	mulps	%xmm8,  %xmm12
	movaps	 -4 * SIZE(X), %xmm8
	addps	%xmm12, %xmm1

	movss	%xmm10, %xmm9
	pshufd	$0xb1,  %xmm5, %xmm12
	shufps	$0x39,  %xmm9, %xmm9
	mulps	%xmm9,  %xmm5
	addps	%xmm5,  %xmm0
	movaps	  4 * SIZE(Y), %xmm5
	mulps	%xmm9,  %xmm12
	movaps	  0 * SIZE(X), %xmm9
	addps	%xmm12, %xmm1

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	movss	%xmm11, %xmm10
	pshufd	$0xb1,  %xmm6,  %xmm12
	shufps	$0x39,  %xmm10, %xmm10
	mulps	%xmm10, %xmm6
	addps	%xmm6,  %xmm0
	movaps	  8 * SIZE(Y), %xmm6
	mulps	%xmm10, %xmm12
	movaps	  4 * SIZE(X), %xmm10
	addps	%xmm12, %xmm1

	movss	%xmm8,  %xmm11
	pshufd	$0xb1,  %xmm7,  %xmm12
	shufps	$0x39,  %xmm11, %xmm11
	mulps	%xmm11, %xmm7
	addps	%xmm7,  %xmm0
	movaps	 12 * SIZE(Y), %xmm7
	mulps	%xmm11, %xmm12
	movaps	  8 * SIZE(X), %xmm11
	addps	%xmm12, %xmm1

	subq	$-32 * SIZE, X
	subq	$-32 * SIZE, Y

	decq	%rax
	jg	.L51
	ALIGN_3

.L52:
	movss	%xmm9,  %xmm8
	pshufd	$0xb1,  %xmm4, %xmm12
	shufps	$0x39,  %xmm8, %xmm8
	mulps	%xmm8,  %xmm4
	addps	%xmm4,  %xmm0
	movaps	-16 * SIZE(Y), %xmm4
	mulps	%xmm8,  %xmm12
	movaps	-20 * SIZE(X), %xmm8
	addps	%xmm12, %xmm1

	movss	%xmm10, %xmm9
	pshufd	$0xb1,  %xmm5, %xmm12
	shufps	$0x39,  %xmm9, %xmm9
	mulps	%xmm9,  %xmm5
	addps	%xmm5,  %xmm0
	movaps	-12 * SIZE(Y), %xmm5
	mulps	%xmm9,  %xmm12
	movaps	-16 * SIZE(X), %xmm9
	addps	%xmm12, %xmm1

	movss	%xmm11, %xmm10
	pshufd	$0xb1,  %xmm6,  %xmm12
	shufps	$0x39,  %xmm10, %xmm10
	mulps	%xmm10, %xmm6
	addps	%xmm6,  %xmm0
	movaps	 -8 * SIZE(Y), %xmm6
	mulps	%xmm10, %xmm12
	movaps	-12 * SIZE(X), %xmm10
	addps	%xmm12, %xmm1

	movss	%xmm8,  %xmm11
	pshufd	$0xb1,  %xmm7,  %xmm12
	shufps	$0x39,  %xmm11, %xmm11
	mulps	%xmm11, %xmm7
	addps	%xmm7,  %xmm0
	movaps	 -4 * SIZE(Y), %xmm7
	mulps	%xmm11, %xmm12
	movaps	 -8 * SIZE(X), %xmm11
	addps	%xmm12, %xmm1

	movss	%xmm9,  %xmm8
	pshufd	$0xb1,  %xmm4, %xmm12
	shufps	$0x39,  %xmm8, %xmm8
	mulps	%xmm8,  %xmm4
	addps	%xmm4,  %xmm0
	mulps	%xmm8,  %xmm12
	movaps	 -4 * SIZE(X), %xmm8
	addps	%xmm12, %xmm1

	movss	%xmm10, %xmm9
	pshufd	$0xb1,  %xmm5, %xmm12
	shufps	$0x39,  %xmm9, %xmm9
	mulps	%xmm9,  %xmm5
	addps	%xmm5,  %xmm0
	mulps	%xmm9,  %xmm12
	addps	%xmm12, %xmm1

	movss	%xmm11, %xmm10
	pshufd	$0xb1,  %xmm6,  %xmm12
	shufps	$0x39,  %xmm10, %xmm10
	mulps	%xmm10, %xmm6
	addps	%xmm6,  %xmm0
	mulps	%xmm10, %xmm12
	addps	%xmm12, %xmm1

	movss	%xmm8,  %xmm11
	pshufd	$0xb1,  %xmm7,  %xmm12
	shufps	$0x39,  %xmm11, %xmm11
	mulps	%xmm11, %xmm7
	addps	%xmm7,  %xmm0
	mulps	%xmm11, %xmm12
	addps	%xmm12, %xmm1

	subq	$-32 * SIZE, X
	subq	$-32 * SIZE, Y
	ALIGN_3

.L55:
	testq	$8, N
	jle	.L56

	movaps	-32 * SIZE(Y), %xmm4
	movaps	-32 * SIZE(X), %xmm9
	movaps	-28 * SIZE(Y), %xmm5
	movaps	-28 * SIZE(X), %xmm10

	movss	%xmm9,  %xmm8
	pshufd	$0xb1,  %xmm4, %xmm12
	shufps	$0x39,  %xmm8, %xmm8
	mulps	%xmm8,  %xmm4
	addps	%xmm4,  %xmm0
	mulps	%xmm8,  %xmm12
	addps	%xmm12, %xmm1

	movaps	-24 * SIZE(Y), %xmm6
	movaps	-24 * SIZE(X), %xmm11

	movss	%xmm10, %xmm9
	pshufd	$0xb1,  %xmm5, %xmm12
	shufps	$0x39,  %xmm9, %xmm9
	mulps	%xmm9,  %xmm5
	addps	%xmm5,  %xmm0
	mulps	%xmm9,  %xmm12
	addps	%xmm12, %xmm1

	movaps	-20 * SIZE(Y), %xmm7
	movaps	-20 * SIZE(X), %xmm8

	movss	%xmm11, %xmm10
	pshufd	$0xb1,  %xmm6,  %xmm12
	shufps	$0x39,  %xmm10, %xmm10
	mulps	%xmm10, %xmm6
	addps	%xmm6,  %xmm0
	mulps	%xmm10, %xmm12
	addps	%xmm12, %xmm1

	movss	%xmm8,  %xmm11
	pshufd	$0xb1,  %xmm7,  %xmm12
	shufps	$0x39,  %xmm11, %xmm11
	mulps	%xmm11, %xmm7
	addps	%xmm7,  %xmm0
	mulps	%xmm11, %xmm12
	addps	%xmm12, %xmm1

	addq	$16 * SIZE, X
	addq	$16 * SIZE, Y
	ALIGN_3

.L56:
	testq	$4, N
	jle	.L57

	movaps	-32 * SIZE(Y), %xmm4
	movaps	-32 * SIZE(X), %xmm9

	movss	%xmm9,  %xmm8
	pshufd	$0xb1,  %xmm4, %xmm12
	shufps	$0x39,  %xmm8, %xmm8
	mulps	%xmm8,  %xmm4
	addps	%xmm4,  %xmm0
	mulps	%xmm8,  %xmm12
	addps	%xmm12, %xmm1

	movaps	-28 * SIZE(Y), %xmm5
	movaps	-28 * SIZE(X), %xmm10

	movss	%xmm10, %xmm9
	pshufd	$0xb1,  %xmm5, %xmm12
	shufps	$0x39,  %xmm9, %xmm9
	mulps	%xmm9,  %xmm5
	addps	%xmm5,  %xmm0
	mulps	%xmm9,  %xmm12
	addps	%xmm12, %xmm1

	movaps	%xmm10, %xmm8

	addq	$8 * SIZE, X
	addq	$8 * SIZE, Y
	ALIGN_3

.L57:
	testq	$2, N
	jle	.L58

	movaps	-32 * SIZE(Y), %xmm4
	movaps	-32 * SIZE(X), %xmm9

	movss	%xmm9,  %xmm8
	pshufd	$0xb1,  %xmm4, %xmm12
	shufps	$0x39,  %xmm8, %xmm8
	mulps	%xmm8,  %xmm4
	addps	%xmm4,  %xmm0
	mulps	%xmm8,  %xmm12
	addps	%xmm12, %xmm1

	movaps	%xmm9, %xmm8
	addq	$4 * SIZE, X
	addq	$4 * SIZE, Y
	ALIGN_3

.L58:
	testq	$1, N
	jle	.L98

#ifdef movsd
	xorps	%xmm4, %xmm4
#endif
	movsd	-32 * SIZE(Y), %xmm4

	pshufd	$0xb1,  %xmm4, %xmm12
	shufps	$0xa9,  %xmm8, %xmm8
	mulps	%xmm8,  %xmm4
	addps	%xmm4,  %xmm0
	mulps	%xmm8,  %xmm12
	addps	%xmm12, %xmm1
	jmp	.L98
	ALIGN_3

.L60:
	movaps	-35 * SIZE(X), %xmm8
	addq	$1 * SIZE, X

	shufps	$0xb1, %xmm1, %xmm1

	movq	N,  %rax
	sarq	$4, %rax
	jle	.L65

	movaps	-32 * SIZE(Y), %xmm4
	movaps	-32 * SIZE(X), %xmm9
	movaps	-28 * SIZE(Y), %xmm5
	movaps	-28 * SIZE(X), %xmm10
	movaps	-24 * SIZE(Y), %xmm6
	movaps	-24 * SIZE(X), %xmm11
	movaps	-20 * SIZE(Y), %xmm7

	decq	%rax
	jle	.L62
	ALIGN_3

.L61:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movss	%xmm9,  %xmm8
	pshufd	$0xb1,  %xmm4, %xmm12
	shufps	$0x93,  %xmm9, %xmm8
	mulps	%xmm8,  %xmm4
	addps	%xmm4,  %xmm0
	movaps	-16 * SIZE(Y), %xmm4
	mulps	%xmm8,  %xmm12
	movaps	-20 * SIZE(X), %xmm8
	addps	%xmm12, %xmm1

	movss	%xmm10, %xmm9
	pshufd	$0xb1,  %xmm5, %xmm12
	shufps	$0x93,  %xmm10, %xmm9
	mulps	%xmm9,  %xmm5
	addps	%xmm5,  %xmm0
	movaps	-12 * SIZE(Y), %xmm5
	mulps	%xmm9,  %xmm12
	movaps	-16 * SIZE(X), %xmm9
	addps	%xmm12, %xmm1

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	movss	%xmm11, %xmm10
	pshufd	$0xb1,  %xmm6,  %xmm12
	shufps	$0x93,  %xmm11, %xmm10
	mulps	%xmm10, %xmm6
	addps	%xmm6,  %xmm0
	movaps	 -8 * SIZE(Y), %xmm6
	mulps	%xmm10, %xmm12
	movaps	-12 * SIZE(X), %xmm10
	addps	%xmm12, %xmm1

	movss	%xmm8,  %xmm11
	pshufd	$0xb1,  %xmm7,  %xmm12
	shufps	$0x93,  %xmm8, %xmm11
	mulps	%xmm11, %xmm7
	addps	%xmm7,  %xmm0
	movaps	 -4 * SIZE(Y), %xmm7
	mulps	%xmm11, %xmm12
	movaps	 -8 * SIZE(X), %xmm11
	addps	%xmm12, %xmm1

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	movss	%xmm9,  %xmm8
	pshufd	$0xb1,  %xmm4, %xmm12
	shufps	$0x93,  %xmm9, %xmm8
	mulps	%xmm8,  %xmm4
	addps	%xmm4,  %xmm0
	movaps	  0 * SIZE(Y), %xmm4
	mulps	%xmm8,  %xmm12
	movaps	 -4 * SIZE(X), %xmm8
	addps	%xmm12, %xmm1

	movss	%xmm10, %xmm9
	pshufd	$0xb1,  %xmm5, %xmm12
	shufps	$0x93,  %xmm10, %xmm9
	mulps	%xmm9,  %xmm5
	addps	%xmm5,  %xmm0
	movaps	  4 * SIZE(Y), %xmm5
	mulps	%xmm9,  %xmm12
	movaps	  0 * SIZE(X), %xmm9
	addps	%xmm12, %xmm1

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	movss	%xmm11, %xmm10
	pshufd	$0xb1,  %xmm6,  %xmm12
	shufps	$0x93,  %xmm11, %xmm10
	mulps	%xmm10, %xmm6
	addps	%xmm6,  %xmm0
	movaps	  8 * SIZE(Y), %xmm6
	mulps	%xmm10, %xmm12
	movaps	  4 * SIZE(X), %xmm10
	addps	%xmm12, %xmm1

	movss	%xmm8,  %xmm11
	pshufd	$0xb1,  %xmm7,  %xmm12
	shufps	$0x93,  %xmm8, %xmm11
	mulps	%xmm11, %xmm7
	addps	%xmm7,  %xmm0
	movaps	 12 * SIZE(Y), %xmm7
	mulps	%xmm11, %xmm12
	movaps	  8 * SIZE(X), %xmm11
	addps	%xmm12, %xmm1

	subq	$-32 * SIZE, X
	subq	$-32 * SIZE, Y

	decq	%rax
	jg	.L61
	ALIGN_3

.L62:
	movss	%xmm9,  %xmm8
	pshufd	$0xb1,  %xmm4, %xmm12
	shufps	$0x93,  %xmm9, %xmm8
	mulps	%xmm8,  %xmm4
	addps	%xmm4,  %xmm0
	movaps	-16 * SIZE(Y), %xmm4
	mulps	%xmm8,  %xmm12
	movaps	-20 * SIZE(X), %xmm8
	addps	%xmm12, %xmm1

	movss	%xmm10, %xmm9
	pshufd	$0xb1,  %xmm5, %xmm12
	shufps	$0x93,  %xmm10, %xmm9
	mulps	%xmm9,  %xmm5
	addps	%xmm5,  %xmm0
	movaps	-12 * SIZE(Y), %xmm5
	mulps	%xmm9,  %xmm12
	movaps	-16 * SIZE(X), %xmm9
	addps	%xmm12, %xmm1

	movss	%xmm11, %xmm10
	pshufd	$0xb1,  %xmm6,  %xmm12
	shufps	$0x93,  %xmm11, %xmm10
	mulps	%xmm10, %xmm6
	addps	%xmm6,  %xmm0
	movaps	 -8 * SIZE(Y), %xmm6
	mulps	%xmm10, %xmm12
	movaps	-12 * SIZE(X), %xmm10
	addps	%xmm12, %xmm1

	movss	%xmm8,  %xmm11
	pshufd	$0xb1,  %xmm7,  %xmm12
	shufps	$0x93,  %xmm8, %xmm11
	mulps	%xmm11, %xmm7
	addps	%xmm7,  %xmm0
	movaps	 -4 * SIZE(Y), %xmm7
	mulps	%xmm11, %xmm12
	movaps	 -8 * SIZE(X), %xmm11
	addps	%xmm12, %xmm1

	movss	%xmm9,  %xmm8
	pshufd	$0xb1,  %xmm4, %xmm12
	shufps	$0x93,  %xmm9, %xmm8
	mulps	%xmm8,  %xmm4
	addps	%xmm4,  %xmm0
	mulps	%xmm8,  %xmm12
	movaps	 -4 * SIZE(X), %xmm8
	addps	%xmm12, %xmm1

	movss	%xmm10, %xmm9
	pshufd	$0xb1,  %xmm5, %xmm12
	shufps	$0x93,  %xmm10, %xmm9
	mulps	%xmm9,  %xmm5
	addps	%xmm5,  %xmm0
	mulps	%xmm9,  %xmm12
	addps	%xmm12, %xmm1

	movss	%xmm11, %xmm10
	pshufd	$0xb1,  %xmm6,  %xmm12
	shufps	$0x93,  %xmm11, %xmm10
	mulps	%xmm10, %xmm6
	addps	%xmm6,  %xmm0
	mulps	%xmm10, %xmm12
	addps	%xmm12, %xmm1

	movss	%xmm8,  %xmm11
	pshufd	$0xb1,  %xmm7,  %xmm12
	shufps	$0x93,  %xmm8, %xmm11
	mulps	%xmm11, %xmm7
	addps	%xmm7,  %xmm0
	mulps	%xmm11, %xmm12
	addps	%xmm12, %xmm1

	subq	$-32 * SIZE, X
	subq	$-32 * SIZE, Y
	ALIGN_3

.L65:
	testq	$8, N
	jle	.L66

	movaps	-32 * SIZE(Y), %xmm4
	movaps	-32 * SIZE(X), %xmm9
	movaps	-28 * SIZE(Y), %xmm5
	movaps	-28 * SIZE(X), %xmm10

	movss	%xmm9,  %xmm8
	pshufd	$0xb1,  %xmm4, %xmm12
	shufps	$0x93,  %xmm9, %xmm8
	mulps	%xmm8,  %xmm4
	addps	%xmm4,  %xmm0
	mulps	%xmm8,  %xmm12
	addps	%xmm12, %xmm1

	movaps	-24 * SIZE(Y), %xmm6
	movaps	-24 * SIZE(X), %xmm11

	movss	%xmm10, %xmm9
	pshufd	$0xb1,  %xmm5, %xmm12
	shufps	$0x93,  %xmm10, %xmm9
	mulps	%xmm9,  %xmm5
	addps	%xmm5,  %xmm0
	mulps	%xmm9,  %xmm12
	addps	%xmm12, %xmm1

	movaps	-20 * SIZE(Y), %xmm7
	movaps	-20 * SIZE(X), %xmm8

	movss	%xmm11, %xmm10
	pshufd	$0xb1,  %xmm6,  %xmm12
	shufps	$0x93,  %xmm11, %xmm10
	mulps	%xmm10, %xmm6
	addps	%xmm6,  %xmm0
	mulps	%xmm10, %xmm12
	addps	%xmm12, %xmm1

	movss	%xmm8,  %xmm11
	pshufd	$0xb1,  %xmm7,  %xmm12
	shufps	$0x93,  %xmm8, %xmm11
	mulps	%xmm11, %xmm7
	addps	%xmm7,  %xmm0
	mulps	%xmm11, %xmm12
	addps	%xmm12, %xmm1

	addq	$16 * SIZE, X
	addq	$16 * SIZE, Y
	ALIGN_3

.L66:
	testq	$4, N
	jle	.L67

	movaps	-32 * SIZE(Y), %xmm4
	movaps	-32 * SIZE(X), %xmm9

	movss	%xmm9,  %xmm8
	pshufd	$0xb1,  %xmm4, %xmm12
	shufps	$0x93,  %xmm9, %xmm8
	mulps	%xmm8,  %xmm4
	addps	%xmm4,  %xmm0
	mulps	%xmm8,  %xmm12
	addps	%xmm12, %xmm1

	movaps	-28 * SIZE(Y), %xmm5
	movaps	-28 * SIZE(X), %xmm10

	movss	%xmm10, %xmm9
	pshufd	$0xb1,  %xmm5, %xmm12
	shufps	$0x93,  %xmm10, %xmm9
	mulps	%xmm9,  %xmm5
	addps	%xmm5,  %xmm0
	mulps	%xmm9,  %xmm12
	addps	%xmm12, %xmm1

	movaps	%xmm10, %xmm8

	addq	$8 * SIZE, X
	addq	$8 * SIZE, Y
	ALIGN_3

.L67:
	testq	$2, N
	jle	.L68

	movaps	-32 * SIZE(Y), %xmm4
	movaps	-32 * SIZE(X), %xmm9

	movss	%xmm9,  %xmm8
	pshufd	$0xb1,  %xmm4, %xmm12
	shufps	$0x93,  %xmm9, %xmm8
	mulps	%xmm8,  %xmm4
	addps	%xmm4,  %xmm0
	mulps	%xmm8,  %xmm12
	addps	%xmm12, %xmm1

	movaps	%xmm9, %xmm8
	addq	$4 * SIZE, X
	addq	$4 * SIZE, Y
	ALIGN_3

.L68:
	testq	$1, N
	jle	.L98

#ifdef movsd
	xorps	%xmm4, %xmm4
#endif
	movsd	-32 * SIZE(Y), %xmm4
	movss	-32 * SIZE(X), %xmm9

	movss	%xmm9,  %xmm8
	pshufd	$0xb1,  %xmm4, %xmm12
	shufps	$0x03,  %xmm8, %xmm8
	mulps	%xmm8,  %xmm4
	addps	%xmm4,  %xmm0
	mulps	%xmm8,  %xmm12
	addps	%xmm12, %xmm1
	jmp	.L98
	ALIGN_3

#else

	testq	$2 * SIZE, Y
	je	.L50x

#ifdef movsd
	xorps	%xmm0, %xmm0
#endif
	movsd	-32 * SIZE(Y), %xmm0
#ifdef movsd
	xorps	%xmm4, %xmm4
#endif
	movsd	-32 * SIZE(X), %xmm4

	pshufd	$0xb1,  %xmm0, %xmm1
	mulps	%xmm4,  %xmm0
	mulps	%xmm4,  %xmm1
	addq	$2 * SIZE, X
	addq	$2 * SIZE, Y

	decq	N
	ALIGN_3

.L50x:
	movq	N,  %rax
	sarq	$4, %rax
	jle	.L55

	movaps	-32 * SIZE(Y), %xmm4
	movlps	-32 * SIZE(X), %xmm8
	movhps	-30 * SIZE(X), %xmm8
	movaps	-28 * SIZE(Y), %xmm5
	movlps	-28 * SIZE(X), %xmm9
	movhps	-26 * SIZE(X), %xmm9

	movaps	-24 * SIZE(Y), %xmm6
	movlps	-24 * SIZE(X), %xmm10
	movhps	-22 * SIZE(X), %xmm10
	movaps	-20 * SIZE(Y), %xmm7
	movlps	-20 * SIZE(X), %xmm11
	movhps	-18 * SIZE(X), %xmm11

	decq	%rax
	jle	.L52
	ALIGN_3

.L51:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	pshufd	$0xb1,  %xmm4, %xmm12
	mulps	%xmm8,  %xmm4
	addps	%xmm4,  %xmm0
	movaps	-16 * SIZE(Y), %xmm4
	mulps	%xmm8,  %xmm12
	movlps	-16 * SIZE(X), %xmm8
	movhps	-14 * SIZE(X), %xmm8
	addps	%xmm12, %xmm1

	pshufd	$0xb1,  %xmm5, %xmm12
	mulps	%xmm9,  %xmm5
	addps	%xmm5,  %xmm0
	movaps	-12 * SIZE(Y), %xmm5
	mulps	%xmm9,  %xmm12
	movlps	-12 * SIZE(X), %xmm9
	movhps	-10 * SIZE(X), %xmm9
	addps	%xmm12, %xmm1

	pshufd	$0xb1,  %xmm6,  %xmm12
	mulps	%xmm10, %xmm6
	addps	%xmm6,  %xmm0
	movaps	 -8 * SIZE(Y), %xmm6
	mulps	%xmm10, %xmm12
	movlps	 -8 * SIZE(X), %xmm10
	movhps	 -6 * SIZE(X), %xmm10
	addps	%xmm12, %xmm1

	pshufd	$0xb1,  %xmm7,  %xmm12
	mulps	%xmm11, %xmm7
	addps	%xmm7,  %xmm0
	movaps	 -4 * SIZE(Y), %xmm7
	mulps	%xmm11, %xmm12
	movlps	 -4 * SIZE(X), %xmm11
	movhps	 -2 * SIZE(X), %xmm11
	addps	%xmm12, %xmm1

	pshufd	$0xb1,  %xmm4, %xmm12
	mulps	%xmm8,  %xmm4
	addps	%xmm4,  %xmm0
	movaps	  0 * SIZE(Y), %xmm4
	mulps	%xmm8,  %xmm12
 	movlps	  0 * SIZE(X), %xmm8
	movhps	  2 * SIZE(X), %xmm8
	addps	%xmm12, %xmm1

	pshufd	$0xb1,  %xmm5, %xmm12
	mulps	%xmm9,  %xmm5
	addps	%xmm5,  %xmm0
	movaps	  4 * SIZE(Y), %xmm5
	mulps	%xmm9,  %xmm12
	movlps	  4 * SIZE(X), %xmm9
	movhps	  6 * SIZE(X), %xmm9
	addps	%xmm12, %xmm1

	pshufd	$0xb1,  %xmm6,  %xmm12
	mulps	%xmm10, %xmm6
	addps	%xmm6,  %xmm0
	movaps	  8 * SIZE(Y), %xmm6
	mulps	%xmm10, %xmm12
	movlps	  8 * SIZE(X), %xmm10
	movhps	 10 * SIZE(X), %xmm10
	addps	%xmm12, %xmm1

	pshufd	$0xb1,  %xmm7,  %xmm12
	mulps	%xmm11, %xmm7
	addps	%xmm7,  %xmm0
	movaps	 12 * SIZE(Y), %xmm7
	mulps	%xmm11, %xmm12
	movlps	 12 * SIZE(X), %xmm11
	movhps	 14 * SIZE(X), %xmm11
	addps	%xmm12, %xmm1

	subq	$-32 * SIZE, X
	subq	$-32 * SIZE, Y

	decq	%rax
	jg	.L51
	ALIGN_3

.L52:
	pshufd	$0xb1,  %xmm4, %xmm12
	mulps	%xmm8,  %xmm4
	addps	%xmm4,  %xmm0
	movaps	-16 * SIZE(Y), %xmm4
	mulps	%xmm8,  %xmm12
	movlps	-16 * SIZE(X), %xmm8
	movhps	-14 * SIZE(X), %xmm8
	addps	%xmm12, %xmm1

	pshufd	$0xb1,  %xmm5, %xmm12
	mulps	%xmm9,  %xmm5
	addps	%xmm5,  %xmm0
	movaps	-12 * SIZE(Y), %xmm5
	mulps	%xmm9,  %xmm12
	movlps	-12 * SIZE(X), %xmm9
	movhps	-10 * SIZE(X), %xmm9
	addps	%xmm12, %xmm1

	pshufd	$0xb1,  %xmm6,  %xmm12
	mulps	%xmm10, %xmm6
	addps	%xmm6,  %xmm0
	movaps	 -8 * SIZE(Y), %xmm6
	mulps	%xmm10, %xmm12
	movlps	 -8 * SIZE(X), %xmm10
	movhps	 -6 * SIZE(X), %xmm10
	addps	%xmm12, %xmm1

	pshufd	$0xb1,  %xmm7,  %xmm12
	mulps	%xmm11, %xmm7
	addps	%xmm7,  %xmm0
	movaps	 -4 * SIZE(Y), %xmm7
	mulps	%xmm11, %xmm12
	movlps	 -4 * SIZE(X), %xmm11
	movhps	 -2 * SIZE(X), %xmm11
	addps	%xmm12, %xmm1

	pshufd	$0xb1,  %xmm4, %xmm12
	mulps	%xmm8,  %xmm4
	addps	%xmm4,  %xmm0
	mulps	%xmm8,  %xmm12
	addps	%xmm12, %xmm1

	pshufd	$0xb1,  %xmm5, %xmm12
	mulps	%xmm9,  %xmm5
	addps	%xmm5,  %xmm0
	mulps	%xmm9,  %xmm12
	addps	%xmm12, %xmm1

	pshufd	$0xb1,  %xmm6,  %xmm12
	mulps	%xmm10, %xmm6
	addps	%xmm6,  %xmm0
	mulps	%xmm10, %xmm12
	addps	%xmm12, %xmm1

	pshufd	$0xb1,  %xmm7,  %xmm12
	mulps	%xmm11, %xmm7
	addps	%xmm7,  %xmm0
	mulps	%xmm11, %xmm12
	addps	%xmm12, %xmm1

	subq	$-32 * SIZE, X
	subq	$-32 * SIZE, Y
	ALIGN_3

.L55:
	testq	$8, N
	jle	.L56

	movaps	-32 * SIZE(Y), %xmm4
	movlps	-32 * SIZE(X), %xmm8
	movhps	-30 * SIZE(X), %xmm8

	movaps	-28 * SIZE(Y), %xmm5
	movlps	-28 * SIZE(X), %xmm9
	movhps	-26 * SIZE(X), %xmm9

	pshufd	$0xb1,  %xmm4, %xmm12
	mulps	%xmm8,  %xmm4
	addps	%xmm4,  %xmm0
	mulps	%xmm8,  %xmm12
	addps	%xmm12, %xmm1

	movaps	-24 * SIZE(Y), %xmm6
	movlps	-24 * SIZE(X), %xmm10
	movhps	-22 * SIZE(X), %xmm10

	pshufd	$0xb1,  %xmm5, %xmm12
	mulps	%xmm9,  %xmm5
	addps	%xmm5,  %xmm0
	mulps	%xmm9,  %xmm12
	addps	%xmm12, %xmm1

	movaps	-20 * SIZE(Y), %xmm7
	movlps	-20 * SIZE(X), %xmm11
	movhps	-18 * SIZE(X), %xmm11

	pshufd	$0xb1,  %xmm6,  %xmm12
	mulps	%xmm10, %xmm6
	addps	%xmm6,  %xmm0
	mulps	%xmm10, %xmm12
	addps	%xmm12, %xmm1

	pshufd	$0xb1,  %xmm7,  %xmm12
	mulps	%xmm11, %xmm7
	addps	%xmm7,  %xmm0
	mulps	%xmm11, %xmm12
	addps	%xmm12, %xmm1

	addq	$16 * SIZE, X
	addq	$16 * SIZE, Y
	ALIGN_3

.L56:
	testq	$4, N
	jle	.L57

	movaps	-32 * SIZE(Y), %xmm4
	movlps	-32 * SIZE(X), %xmm8
	movhps	-30 * SIZE(X), %xmm8

	pshufd	$0xb1,  %xmm4, %xmm12
	mulps	%xmm8,  %xmm4
	addps	%xmm4,  %xmm0
	mulps	%xmm8,  %xmm12
	addps	%xmm12, %xmm1

	movaps	-28 * SIZE(Y), %xmm5
	movlps	-28 * SIZE(X), %xmm9
	movhps	-26 * SIZE(X), %xmm9

	pshufd	$0xb1,  %xmm5, %xmm12
	mulps	%xmm9,  %xmm5
	addps	%xmm5,  %xmm0
	mulps	%xmm9,  %xmm12
	addps	%xmm12, %xmm1

	addq	$8 * SIZE, X
	addq	$8 * SIZE, Y
	ALIGN_3

.L57:
	testq	$2, N
	jle	.L58

	movaps	-32 * SIZE(Y), %xmm4
	movlps	-32 * SIZE(X), %xmm8
	movhps	-30 * SIZE(X), %xmm8

	pshufd	$0xb1,  %xmm4, %xmm12
	mulps	%xmm8,  %xmm4
	addps	%xmm4,  %xmm0
	mulps	%xmm8,  %xmm12
	addps	%xmm12, %xmm1

	movaps	%xmm9, %xmm8
	addq	$4 * SIZE, X
	addq	$4 * SIZE, Y
	ALIGN_3

.L58:
	testq	$1, N
	jle	.L98

#ifdef movsd
	xorps	%xmm4, %xmm4
#endif
	movsd	-32 * SIZE(Y), %xmm4
#ifdef movsd
	xorps	%xmm8, %xmm8
#endif
	movsd	-32 * SIZE(X), %xmm8

	pshufd	$0xb1,  %xmm4, %xmm12
	mulps	%xmm8,  %xmm4
	addps	%xmm4,  %xmm0
	mulps	%xmm8,  %xmm12
	addps	%xmm12, %xmm1
	jmp	.L98
	ALIGN_3
#endif

.L70:
	testq	$2 * SIZE, Y
	je	.L70x

#ifdef movsd
	xorps	%xmm4, %xmm4
#endif
	movsd	-32 * SIZE(X), %xmm4
	addq	$2 * SIZE, X
#ifdef movsd
	xorps	%xmm1, %xmm1
#endif
	movsd	-32 * SIZE(Y), %xmm1
	addq	$2 * SIZE, Y

	pshufd	$0xb1, %xmm1, %xmm0
	shufps	$0xb1, %xmm4, %xmm4

	mulps	%xmm4,  %xmm0
	mulps	%xmm4,  %xmm1
	decq	N
	ALIGN_3

.L70x:
	testq	$2 * SIZE, X
	jne	.L80

	movaps	-33 * SIZE(X), %xmm4
	addq	$3 * SIZE, X
	movaps	-33 * SIZE(Y), %xmm8
	addq	$3 * SIZE, Y

	movq	N,  %rax
	sarq	$4, %rax
	jle	.L75

	movaps	-32 * SIZE(X), %xmm5
	movaps	-32 * SIZE(Y), %xmm9
	movaps	-28 * SIZE(X), %xmm6
	movaps	-28 * SIZE(Y), %xmm10
	movaps	-24 * SIZE(X), %xmm7
	movaps	-24 * SIZE(Y), %xmm11

	decq	%rax
	jle	.L72
	ALIGN_3

.L71:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movss	%xmm9,  %xmm8
	pshufd	$0x1b,  %xmm8, %xmm12
	movss	%xmm5,  %xmm4
	mulps	%xmm4,  %xmm8
	addps	%xmm8,  %xmm0
	movaps	-20 * SIZE(Y), %xmm8
	mulps	%xmm4,  %xmm12
	movaps	-20 * SIZE(X), %xmm4
	addps	%xmm12, %xmm1

	movss	%xmm10, %xmm9
	pshufd	$0x1b,  %xmm9, %xmm12
	movss	%xmm6,  %xmm5
	mulps	%xmm5,  %xmm9
	addps	%xmm9,  %xmm2
	movaps	-16 * SIZE(Y), %xmm9
	mulps	%xmm5,  %xmm12
	movaps	-16 * SIZE(X), %xmm5
	addps	%xmm12, %xmm3

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	movss	%xmm11, %xmm10
	pshufd	$0x1b,  %xmm10, %xmm12
	movss	%xmm7,  %xmm6
	mulps	%xmm6,  %xmm10
	addps	%xmm10, %xmm0
	movaps	-12 * SIZE(Y), %xmm10
	mulps	%xmm6,  %xmm12
	movaps	-12 * SIZE(X), %xmm6
	addps	%xmm12, %xmm1

	movss	%xmm8,  %xmm11
	pshufd	$0x1b,  %xmm11, %xmm12
	movss	%xmm4,  %xmm7
	mulps	%xmm7,  %xmm11
	addps	%xmm11, %xmm2
	movaps	 -8 * SIZE(Y), %xmm11
	mulps	%xmm7,  %xmm12
	movaps	 -8 * SIZE(X), %xmm7
	addps	%xmm12, %xmm3

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	movss	%xmm9,  %xmm8
	pshufd	$0x1b,  %xmm8, %xmm12
	movss	%xmm5,  %xmm4
	mulps	%xmm4,  %xmm8
	addps	%xmm8,  %xmm0
 	movaps	 -4 * SIZE(Y), %xmm8
	mulps	%xmm4,  %xmm12
	movaps	 -4 * SIZE(X), %xmm4
	addps	%xmm12, %xmm1

	movss	%xmm10, %xmm9
	pshufd	$0x1b,  %xmm9, %xmm12
	movss	%xmm6,  %xmm5
	mulps	%xmm5,  %xmm9
	addps	%xmm9,  %xmm2
	movaps	  0 * SIZE(Y), %xmm9
	mulps	%xmm5,  %xmm12
	movaps	  0 * SIZE(X), %xmm5
	addps	%xmm12, %xmm3

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	movss	%xmm11, %xmm10
	pshufd	$0x1b,  %xmm10, %xmm12
	movss	%xmm7,  %xmm6
	mulps	%xmm6,  %xmm10
	addps	%xmm10, %xmm0
	movaps	  4 * SIZE(Y), %xmm10
	mulps	%xmm6,  %xmm12
	movaps	  4 * SIZE(X), %xmm6
	addps	%xmm12, %xmm1

	movss	%xmm8,  %xmm11
	pshufd	$0x1b,  %xmm11, %xmm12
	movss	%xmm4,  %xmm7
	mulps	%xmm7,  %xmm11
	addps	%xmm11, %xmm2
	movaps	  8 * SIZE(Y), %xmm11
	mulps	%xmm7,  %xmm12
	movaps	  8 * SIZE(X), %xmm7
	addps	%xmm12, %xmm3

	subq	$-32 * SIZE, X
	subq	$-32 * SIZE, Y

	decq	%rax
	jg	.L71
	ALIGN_3

.L72:
	movss	%xmm9,  %xmm8
	pshufd	$0x1b,  %xmm8, %xmm12
	movss	%xmm5,  %xmm4
	mulps	%xmm4,  %xmm8
	addps	%xmm8,  %xmm0
	movaps	-20 * SIZE(Y), %xmm8
	mulps	%xmm4,  %xmm12
	movaps	-20 * SIZE(X), %xmm4
	addps	%xmm12, %xmm1

	movss	%xmm10, %xmm9
	pshufd	$0x1b,  %xmm9, %xmm12
	movss	%xmm6,  %xmm5
	mulps	%xmm5,  %xmm9
	addps	%xmm9,  %xmm2
	movaps	-16 * SIZE(Y), %xmm9
	mulps	%xmm5,  %xmm12
	movaps	-16 * SIZE(X), %xmm5
	addps	%xmm12, %xmm3

	movss	%xmm11, %xmm10
	pshufd	$0x1b,  %xmm10, %xmm12
	movss	%xmm7,  %xmm6
	mulps	%xmm6,  %xmm10
	addps	%xmm10, %xmm0
	movaps	-12 * SIZE(Y), %xmm10
	mulps	%xmm6,  %xmm12
	movaps	-12 * SIZE(X), %xmm6
	addps	%xmm12, %xmm1

	movss	%xmm8,  %xmm11
	pshufd	$0x1b,  %xmm11, %xmm12
	movss	%xmm4,  %xmm7
	mulps	%xmm7,  %xmm11
	addps	%xmm11, %xmm2
	movaps	 -8 * SIZE(Y), %xmm11
	mulps	%xmm7,  %xmm12
	movaps	 -8 * SIZE(X), %xmm7
	addps	%xmm12, %xmm3

	movss	%xmm9,  %xmm8
	pshufd	$0x1b,  %xmm8, %xmm12
	movss	%xmm5,  %xmm4
	mulps	%xmm4,  %xmm8
	addps	%xmm8,  %xmm0
 	movaps	 -4 * SIZE(Y), %xmm8
	mulps	%xmm4,  %xmm12
	movaps	 -4 * SIZE(X), %xmm4
	addps	%xmm12, %xmm1

	movss	%xmm10, %xmm9
	pshufd	$0x1b,  %xmm9, %xmm12
	movss	%xmm6,  %xmm5
	mulps	%xmm5,  %xmm9
	addps	%xmm9,  %xmm2
	mulps	%xmm5,  %xmm12
	addps	%xmm12, %xmm3

	movss	%xmm11, %xmm10
	pshufd	$0x1b,  %xmm10, %xmm12
	movss	%xmm7,  %xmm6
	mulps	%xmm6,  %xmm10
	addps	%xmm10, %xmm0
	mulps	%xmm6,  %xmm12
	addps	%xmm12, %xmm1

	movss	%xmm8,  %xmm11
	pshufd	$0x1b,  %xmm11, %xmm12
	movss	%xmm4,  %xmm7
	mulps	%xmm7,  %xmm11
	addps	%xmm11, %xmm2
	mulps	%xmm7,  %xmm12
	addps	%xmm12, %xmm3

	subq	$-32 * SIZE, X
	subq	$-32 * SIZE, Y
	ALIGN_3

.L75:
	testq	$8, N
	jle	.L76

	movaps	-32 * SIZE(X), %xmm5
	movaps	-32 * SIZE(Y), %xmm9

	movss	%xmm9,  %xmm8
	pshufd	$0x1b,  %xmm8, %xmm12
	movss	%xmm5,  %xmm4
	mulps	%xmm4,  %xmm8
	addps	%xmm8,  %xmm0
	mulps	%xmm4,  %xmm12
	addps	%xmm12, %xmm1

	movaps	-28 * SIZE(X), %xmm6
	movaps	-28 * SIZE(Y), %xmm10

	movss	%xmm10, %xmm9
	pshufd	$0x1b,  %xmm9, %xmm12
	movss	%xmm6,  %xmm5
	mulps	%xmm5,  %xmm9
	addps	%xmm9,  %xmm2
	mulps	%xmm5,  %xmm12
	addps	%xmm12, %xmm3

	movaps	-24 * SIZE(X), %xmm7
	movaps	-24 * SIZE(Y), %xmm11

	movss	%xmm11, %xmm10
	pshufd	$0x1b,  %xmm10, %xmm12
	movss	%xmm7,  %xmm6
	mulps	%xmm6,  %xmm10
	addps	%xmm10, %xmm0
	mulps	%xmm6,  %xmm12
	addps	%xmm12, %xmm1

	movaps	-20 * SIZE(X), %xmm4
	movaps	-20 * SIZE(Y), %xmm8

	movss	%xmm8,  %xmm11
	pshufd	$0x1b,  %xmm11, %xmm12
	movss	%xmm4,  %xmm7
	mulps	%xmm7,  %xmm11
	addps	%xmm11, %xmm2
	mulps	%xmm7,  %xmm12
	addps	%xmm12, %xmm3

	addq	$16 * SIZE, X
	addq	$16 * SIZE, Y
	ALIGN_3

.L76:
	testq	$4, N
	jle	.L77

	movaps	-32 * SIZE(X), %xmm5
	movaps	-32 * SIZE(Y), %xmm9
	movaps	-28 * SIZE(X), %xmm6
	movaps	-28 * SIZE(Y), %xmm10

	movss	%xmm9,  %xmm8
	pshufd	$0x1b,  %xmm8, %xmm12
	movss	%xmm5,  %xmm4
	mulps	%xmm4,  %xmm8
	addps	%xmm8,  %xmm0
	mulps	%xmm4,  %xmm12
	addps	%xmm12, %xmm1

	movss	%xmm10, %xmm9
	pshufd	$0x1b,  %xmm9, %xmm12
	movss	%xmm6,  %xmm5
	mulps	%xmm5,  %xmm9
	addps	%xmm9,  %xmm2
	mulps	%xmm5,  %xmm12
	addps	%xmm12, %xmm3

	movaps	%xmm6, %xmm4
	movaps	%xmm10, %xmm8

	addq	$8 * SIZE, X
	addq	$8 * SIZE, Y
	ALIGN_3

.L77:
	testq	$2, N
	jle	.L78

	movaps	-32 * SIZE(X), %xmm5
	movaps	-32 * SIZE(Y), %xmm9

	movss	%xmm9,  %xmm8
	pshufd	$0x1b,  %xmm8, %xmm12
	movss	%xmm5,  %xmm4
	mulps	%xmm4,  %xmm8
	addps	%xmm8,  %xmm0
	mulps	%xmm4,  %xmm12
	addps	%xmm12, %xmm1

	movaps	%xmm5, %xmm4
	movaps	%xmm9, %xmm8
	ALIGN_3

.L78:
	testq	$1, N
	jle	.L79

	xorps	%xmm5, %xmm5
	movss	%xmm5, %xmm4
	movss	%xmm5, %xmm8

	shufps	$0x24, %xmm4, %xmm4
	pshufd	$0x18, %xmm8, %xmm12
	shufps	$0x24, %xmm8, %xmm8

	mulps	%xmm4,  %xmm8
	addps	%xmm8,  %xmm0
	mulps	%xmm4,  %xmm12
	addps	%xmm12, %xmm1
	ALIGN_3

.L79:
	shufps	$0x39, %xmm0, %xmm0
	shufps	$0x39, %xmm1, %xmm1
	shufps	$0x39, %xmm2, %xmm2
	shufps	$0x39, %xmm3, %xmm3
	jmp	.L98
	ALIGN_3

.L80:
	movsd	-33 * SIZE(X), %xmm4
	movhps	-31 * SIZE(X), %xmm4
	addq	$3 * SIZE, X
	movaps	-33 * SIZE(Y), %xmm8
	addq	$3 * SIZE, Y

	movq	N,  %rax
	sarq	$4, %rax
	jle	.L85

	movsd	-32 * SIZE(X), %xmm5
	movhps	-30 * SIZE(X), %xmm5
	movaps	-32 * SIZE(Y), %xmm9

	movsd	-28 * SIZE(X), %xmm6
	movhps	-26 * SIZE(X), %xmm6
	movaps	-28 * SIZE(Y), %xmm10

	movsd	-24 * SIZE(X), %xmm7
	movhps	-22 * SIZE(X), %xmm7
	movaps	-24 * SIZE(Y), %xmm11

	decq	%rax
	jle	.L82
	ALIGN_3

.L81:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movss	%xmm9,  %xmm8
	pshufd	$0x1b,  %xmm8, %xmm12
	movss	%xmm5,  %xmm4
	mulps	%xmm4,  %xmm8
	addps	%xmm8,  %xmm0
	movaps	-20 * SIZE(Y), %xmm8
	mulps	%xmm4,  %xmm12
	movsd	-20 * SIZE(X), %xmm4
	movhps	-18 * SIZE(X), %xmm4
	addps	%xmm12, %xmm1

	movss	%xmm10, %xmm9
	pshufd	$0x1b,  %xmm9, %xmm12
	movss	%xmm6,  %xmm5
	mulps	%xmm5,  %xmm9
	addps	%xmm9,  %xmm2
	movaps	-16 * SIZE(Y), %xmm9
	mulps	%xmm5,  %xmm12
	movsd	-16 * SIZE(X), %xmm5
	movhps	-14 * SIZE(X), %xmm5
	addps	%xmm12, %xmm3

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	movss	%xmm11, %xmm10
	pshufd	$0x1b,  %xmm10, %xmm12
	movss	%xmm7,  %xmm6
	mulps	%xmm6,  %xmm10
	addps	%xmm10, %xmm0
	movaps	-12 * SIZE(Y), %xmm10
	mulps	%xmm6,  %xmm12
	movsd	-12 * SIZE(X), %xmm6
	movhps	-10 * SIZE(X), %xmm6
	addps	%xmm12, %xmm1

	movss	%xmm8,  %xmm11
	pshufd	$0x1b,  %xmm11, %xmm12
	movss	%xmm4,  %xmm7
	mulps	%xmm7,  %xmm11
	addps	%xmm11, %xmm2
	movaps	 -8 * SIZE(Y), %xmm11
	mulps	%xmm7,  %xmm12
	movsd	 -8 * SIZE(X), %xmm7
	movhps	 -6 * SIZE(X), %xmm7
	addps	%xmm12, %xmm3

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	movss	%xmm9,  %xmm8
	pshufd	$0x1b,  %xmm8, %xmm12
	movss	%xmm5,  %xmm4
	mulps	%xmm4,  %xmm8
	addps	%xmm8,  %xmm0
 	movaps	 -4 * SIZE(Y), %xmm8
	mulps	%xmm4,  %xmm12
	movsd	 -4 * SIZE(X), %xmm4
	movhps	 -2 * SIZE(X), %xmm4
	addps	%xmm12, %xmm1

	movss	%xmm10, %xmm9
	pshufd	$0x1b,  %xmm9, %xmm12
	movss	%xmm6,  %xmm5
	mulps	%xmm5,  %xmm9
	addps	%xmm9,  %xmm2
	movaps	  0 * SIZE(Y), %xmm9
	mulps	%xmm5,  %xmm12
	movsd	  0 * SIZE(X), %xmm5
	movhps	  2 * SIZE(X), %xmm5
	addps	%xmm12, %xmm3

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	movss	%xmm11, %xmm10
	pshufd	$0x1b,  %xmm10, %xmm12
	movss	%xmm7,  %xmm6
	mulps	%xmm6,  %xmm10
	addps	%xmm10, %xmm0
	movaps	  4 * SIZE(Y), %xmm10
	mulps	%xmm6,  %xmm12
	movsd	  4 * SIZE(X), %xmm6
	movhps	  6 * SIZE(X), %xmm6
	addps	%xmm12, %xmm1

	movss	%xmm8,  %xmm11
	pshufd	$0x1b,  %xmm11, %xmm12
	movss	%xmm4,  %xmm7
	mulps	%xmm7,  %xmm11
	addps	%xmm11, %xmm2
	movaps	  8 * SIZE(Y), %xmm11
	mulps	%xmm7,  %xmm12
	movsd	  8 * SIZE(X), %xmm7
	movhps	 10 * SIZE(X), %xmm7
	addps	%xmm12, %xmm3

	subq	$-32 * SIZE, X
	subq	$-32 * SIZE, Y

	decq	%rax
	jg	.L81
	ALIGN_3

.L82:
	movss	%xmm9,  %xmm8
	pshufd	$0x1b,  %xmm8, %xmm12
	movss	%xmm5,  %xmm4
	mulps	%xmm4,  %xmm8
	addps	%xmm8,  %xmm0
	movaps	-20 * SIZE(Y), %xmm8
	mulps	%xmm4,  %xmm12
	movsd	-20 * SIZE(X), %xmm4
	movhps	-18 * SIZE(X), %xmm4
	addps	%xmm12, %xmm1

	movss	%xmm10, %xmm9
	pshufd	$0x1b,  %xmm9, %xmm12
	movss	%xmm6,  %xmm5
	mulps	%xmm5,  %xmm9
	addps	%xmm9,  %xmm2
	movaps	-16 * SIZE(Y), %xmm9
	mulps	%xmm5,  %xmm12
	movsd	-16 * SIZE(X), %xmm5
	movhps	-14 * SIZE(X), %xmm5
	addps	%xmm12, %xmm3

	movss	%xmm11, %xmm10
	pshufd	$0x1b,  %xmm10, %xmm12
	movss	%xmm7,  %xmm6
	mulps	%xmm6,  %xmm10
	addps	%xmm10, %xmm0
	movaps	-12 * SIZE(Y), %xmm10
	mulps	%xmm6,  %xmm12
	movsd	-12 * SIZE(X), %xmm6
	movhps	-10 * SIZE(X), %xmm6
	addps	%xmm12, %xmm1

	movss	%xmm8,  %xmm11
	pshufd	$0x1b,  %xmm11, %xmm12
	movss	%xmm4,  %xmm7
	mulps	%xmm7,  %xmm11
	addps	%xmm11, %xmm2
	movaps	 -8 * SIZE(Y), %xmm11
	mulps	%xmm7,  %xmm12
	movsd	 -8 * SIZE(X), %xmm7
	movhps	 -6 * SIZE(X), %xmm7
	addps	%xmm12, %xmm3

	movss	%xmm9,  %xmm8
	pshufd	$0x1b,  %xmm8, %xmm12
	movss	%xmm5,  %xmm4
	mulps	%xmm4,  %xmm8
	addps	%xmm8,  %xmm0
 	movaps	 -4 * SIZE(Y), %xmm8
	mulps	%xmm4,  %xmm12
	movsd	 -4 * SIZE(X), %xmm4
	movhps	 -2 * SIZE(X), %xmm4
	addps	%xmm12, %xmm1

	movss	%xmm10, %xmm9
	pshufd	$0x1b,  %xmm9, %xmm12
	movss	%xmm6,  %xmm5
	mulps	%xmm5,  %xmm9
	addps	%xmm9,  %xmm2
	mulps	%xmm5,  %xmm12
	addps	%xmm12, %xmm3

	movss	%xmm11, %xmm10
	pshufd	$0x1b,  %xmm10, %xmm12
	movss	%xmm7,  %xmm6
	mulps	%xmm6,  %xmm10
	addps	%xmm10, %xmm0
	mulps	%xmm6,  %xmm12
	addps	%xmm12, %xmm1

	movss	%xmm8,  %xmm11
	pshufd	$0x1b,  %xmm11, %xmm12
	movss	%xmm4,  %xmm7
	mulps	%xmm7,  %xmm11
	addps	%xmm11, %xmm2
	mulps	%xmm7,  %xmm12
	addps	%xmm12, %xmm3

	subq	$-32 * SIZE, X
	subq	$-32 * SIZE, Y
	ALIGN_3

.L85:
	testq	$8, N
	jle	.L86

	movsd	-32 * SIZE(X), %xmm5
	movhps	-30 * SIZE(X), %xmm5
	movaps	-32 * SIZE(Y), %xmm9

	movss	%xmm9,  %xmm8
	pshufd	$0x1b,  %xmm8, %xmm12
	movss	%xmm5,  %xmm4
	mulps	%xmm4,  %xmm8
	addps	%xmm8,  %xmm0
	mulps	%xmm4,  %xmm12
	addps	%xmm12, %xmm1

	movsd	-28 * SIZE(X), %xmm6
	movhps	-26 * SIZE(X), %xmm6
	movaps	-28 * SIZE(Y), %xmm10

	movss	%xmm10, %xmm9
	pshufd	$0x1b,  %xmm9, %xmm12
	movss	%xmm6,  %xmm5
	mulps	%xmm5,  %xmm9
	addps	%xmm9,  %xmm2
	mulps	%xmm5,  %xmm12
	addps	%xmm12, %xmm3

	movsd	-24 * SIZE(X), %xmm7
	movhps	-22 * SIZE(X), %xmm7
	movaps	-24 * SIZE(Y), %xmm11

	movss	%xmm11, %xmm10
	pshufd	$0x1b,  %xmm10, %xmm12
	movss	%xmm7,  %xmm6
	mulps	%xmm6,  %xmm10
	addps	%xmm10, %xmm0
	mulps	%xmm6,  %xmm12
	addps	%xmm12, %xmm1

	movsd	-20 * SIZE(X), %xmm4
	movhps	-18 * SIZE(X), %xmm4
	movaps	-20 * SIZE(Y), %xmm8

	movss	%xmm8,  %xmm11
	pshufd	$0x1b,  %xmm11, %xmm12
	movss	%xmm4,  %xmm7
	mulps	%xmm7,  %xmm11
	addps	%xmm11, %xmm2
	mulps	%xmm7,  %xmm12
	addps	%xmm12, %xmm3

	addq	$16 * SIZE, X
	addq	$16 * SIZE, Y
	ALIGN_3

.L86:
	testq	$4, N
	jle	.L87

	movsd	-32 * SIZE(X), %xmm5
	movhps	-30 * SIZE(X), %xmm5
	movaps	-32 * SIZE(Y), %xmm9

	movss	%xmm9,  %xmm8
	pshufd	$0x1b,  %xmm8, %xmm12
	movss	%xmm5,  %xmm4
	mulps	%xmm4,  %xmm8
	addps	%xmm8,  %xmm0
	mulps	%xmm4,  %xmm12
	addps	%xmm12, %xmm1

	movsd	-28 * SIZE(X), %xmm6
	movhps	-26 * SIZE(X), %xmm6
	movaps	-28 * SIZE(Y), %xmm10

	movss	%xmm10, %xmm9
	pshufd	$0x1b,  %xmm9, %xmm12
	movss	%xmm6,  %xmm5
	mulps	%xmm5,  %xmm9
	addps	%xmm9,  %xmm2
	mulps	%xmm5,  %xmm12
	addps	%xmm12, %xmm3

	movaps	%xmm6, %xmm4
	movaps	%xmm10, %xmm8

	addq	$8 * SIZE, X
	addq	$8 * SIZE, Y
	ALIGN_3

.L87:
	testq	$2, N
	jle	.L88

	movsd	-32 * SIZE(X), %xmm5
	movhps	-30 * SIZE(X), %xmm5
	movaps	-32 * SIZE(Y), %xmm9

	movss	%xmm9,  %xmm8
	pshufd	$0x1b,  %xmm8, %xmm12
	movss	%xmm5,  %xmm4
	mulps	%xmm4,  %xmm8
	addps	%xmm8,  %xmm0
	mulps	%xmm4,  %xmm12
	addps	%xmm12, %xmm1

	movaps	%xmm5, %xmm4
	movaps	%xmm9, %xmm8
	ALIGN_3

.L88:
	testq	$1, N
	jle	.L89

	xorps	%xmm5, %xmm5
	movss	%xmm5, %xmm4
	movss	%xmm5, %xmm8

	shufps	$0x24, %xmm4, %xmm4
	pshufd	$0x18, %xmm8, %xmm12
	shufps	$0x24, %xmm8, %xmm8

	mulps	%xmm4,  %xmm8
	addps	%xmm8,  %xmm0
	mulps	%xmm4,  %xmm12
	addps	%xmm12, %xmm1
	ALIGN_3

.L89:
	shufps	$0x39, %xmm0, %xmm0
	shufps	$0x39, %xmm1, %xmm1
	shufps	$0x39, %xmm2, %xmm2
	shufps	$0x39, %xmm3, %xmm3
	jmp	.L98
	ALIGN_3

.L200:
	movq	N,  %rax
	sarq	$4, %rax
	jle	.L205

	movsd	(X), %xmm4
	addq	INCX, X
	movhps	(X), %xmm4
	addq	INCX, X
	movsd	(Y), %xmm8
	addq	INCY, Y
	movhps	(Y), %xmm8
	addq	INCY, Y

	movsd	(X), %xmm5
	addq	INCX, X
	movhps	(X), %xmm5
	addq	INCX, X
	movsd	(Y), %xmm9
	addq	INCY, Y
	movhps	(Y), %xmm9
	addq	INCY, Y

	movsd	(X), %xmm6
	addq	INCX, X
	movhps	(X), %xmm6
	addq	INCX, X
	movsd	(Y), %xmm10
	addq	INCY, Y
	movhps	(Y), %xmm10
	addq	INCY, Y

	movsd	(X), %xmm7
	addq	INCX, X
	movhps	(X), %xmm7
	addq	INCX, X
	movsd	(Y), %xmm11
	addq	INCY, Y
	movhps	(Y), %xmm11
	addq	INCY, Y

	decq	%rax
	jle	.L204
	ALIGN_3

.L203:
	pshufd	$0xb1,  %xmm8, %xmm12
	mulps	%xmm4,  %xmm8
	addps	%xmm8,  %xmm0
	movsd	(Y), %xmm8
	addq	INCY, Y
	movhps	(Y), %xmm8
	addq	INCY, Y
	mulps	%xmm4,  %xmm12
	movsd	(X), %xmm4
	addq	INCX, X
	movhps	(X), %xmm4
	addq	INCX, X
	addps	%xmm12, %xmm1

	pshufd	$0xb1,  %xmm9, %xmm12
	mulps	%xmm5,  %xmm9
	addps	%xmm9,  %xmm2
	movsd	(Y), %xmm9
	addq	INCY, Y
	movhps	(Y), %xmm9
	addq	INCY, Y
	mulps	%xmm5,  %xmm12
	movsd	(X), %xmm5
	addq	INCX, X
	movhps	(X), %xmm5
	addq	INCX, X
	addps	%xmm12, %xmm3

	pshufd	$0xb1,  %xmm10, %xmm12
	mulps	%xmm6,  %xmm10
	addps	%xmm10, %xmm0
	movsd	(Y), %xmm10
	addq	INCY, Y
	movhps	(Y), %xmm10
	addq	INCY, Y
	mulps	%xmm6,  %xmm12
	movsd	(X), %xmm6
	addq	INCX, X
	movhps	(X), %xmm6
	addq	INCX, X
	addps	%xmm12, %xmm1

	pshufd	$0xb1,  %xmm11, %xmm12
	mulps	%xmm7,  %xmm11
	addps	%xmm11, %xmm2
	movsd	(Y), %xmm11
	addq	INCY, Y
	movhps	(Y), %xmm11
	addq	INCY, Y
	mulps	%xmm7,  %xmm12
	movsd	(X), %xmm7
	addq	INCX, X
	movhps	(X), %xmm7
	addq	INCX, X
	addps	%xmm12, %xmm3

	pshufd	$0xb1,  %xmm8, %xmm12
	mulps	%xmm4,  %xmm8
	addps	%xmm8,  %xmm0
	movsd	(Y), %xmm8
	addq	INCY, Y
	movhps	(Y), %xmm8
	addq	INCY, Y
	mulps	%xmm4,  %xmm12
	movsd	(X), %xmm4
	addq	INCX, X
	movhps	(X), %xmm4
	addq	INCX, X
	addps	%xmm12, %xmm1

	pshufd	$0xb1,  %xmm9, %xmm12
	mulps	%xmm5,  %xmm9
	addps	%xmm9,  %xmm2
	movsd	(Y), %xmm9
	addq	INCY, Y
	movhps	(Y), %xmm9
	addq	INCY, Y
	mulps	%xmm5,  %xmm12
	movsd	(X), %xmm5
	addq	INCX, X
	movhps	(X), %xmm5
	addq	INCX, X
	addps	%xmm12, %xmm3

	pshufd	$0xb1,  %xmm10, %xmm12
	mulps	%xmm6,  %xmm10
	addps	%xmm10, %xmm0
	movsd	(Y), %xmm10
	addq	INCY, Y
	movhps	(Y), %xmm10
	addq	INCY, Y
	mulps	%xmm6,  %xmm12
	movsd	(X), %xmm6
	addq	INCX, X
	movhps	(X), %xmm6
	addq	INCX, X
	addps	%xmm12, %xmm1

	pshufd	$0xb1,  %xmm11, %xmm12
	mulps	%xmm7,  %xmm11
	addps	%xmm11, %xmm2
	movsd	(Y), %xmm11
	addq	INCY, Y
	movhps	(Y), %xmm11
	addq	INCY, Y

	mulps	%xmm7,  %xmm12
	movsd	(X), %xmm7
	addq	INCX, X
	movhps	(X), %xmm7
	addq	INCX, X
	addps	%xmm12, %xmm3

	decq	%rax
	jg	.L203
	ALIGN_3

.L204:
	pshufd	$0xb1,  %xmm8, %xmm12
	mulps	%xmm4,  %xmm8
	addps	%xmm8,  %xmm0
	movsd	(Y), %xmm8
	addq	INCY, Y
	movhps	(Y), %xmm8
	addq	INCY, Y
	mulps	%xmm4,  %xmm12
	movsd	(X), %xmm4
	addq	INCX, X
	movhps	(X), %xmm4
	addq	INCX, X
	addps	%xmm12, %xmm1

	pshufd	$0xb1,  %xmm9, %xmm12
	mulps	%xmm5,  %xmm9
	addps	%xmm9,  %xmm2
	movsd	(Y), %xmm9
	addq	INCY, Y
	movhps	(Y), %xmm9
	addq	INCY, Y
	mulps	%xmm5,  %xmm12
	movsd	(X), %xmm5
	addq	INCX, X
	movhps	(X), %xmm5
	addq	INCX, X
	addps	%xmm12, %xmm3

	pshufd	$0xb1,  %xmm10, %xmm12
	mulps	%xmm6,  %xmm10
	addps	%xmm10, %xmm0
	movsd	(Y), %xmm10
	addq	INCY, Y
	movhps	(Y), %xmm10
	addq	INCY, Y
	mulps	%xmm6,  %xmm12
	movsd	(X), %xmm6
	addq	INCX, X
	movhps	(X), %xmm6
	addq	INCX, X
	addps	%xmm12, %xmm1

	pshufd	$0xb1,  %xmm11, %xmm12
	mulps	%xmm7,  %xmm11
	addps	%xmm11, %xmm2
	movsd	(Y), %xmm11
	addq	INCY, Y
	movhps	(Y), %xmm11
	addq	INCY, Y
	mulps	%xmm7,  %xmm12
	movsd	(X), %xmm7
	addq	INCX, X
	movhps	(X), %xmm7
	addq	INCX, X
	addps	%xmm12, %xmm3

	pshufd	$0xb1,  %xmm8, %xmm12
	mulps	%xmm4,  %xmm8
	addps	%xmm8,  %xmm0
	mulps	%xmm4,  %xmm12
	addps	%xmm12, %xmm1

	pshufd	$0xb1,  %xmm9, %xmm12
	mulps	%xmm5,  %xmm9
	addps	%xmm9,  %xmm2
	mulps	%xmm5,  %xmm12
	addps	%xmm12, %xmm3

	pshufd	$0xb1,  %xmm10, %xmm12
	mulps	%xmm6,  %xmm10
	addps	%xmm10, %xmm0
	mulps	%xmm6,  %xmm12
	addps	%xmm12, %xmm1

	pshufd	$0xb1,  %xmm11, %xmm12
	mulps	%xmm7,  %xmm11
	addps	%xmm11, %xmm2
	mulps	%xmm7,  %xmm12
	addps	%xmm12, %xmm3
	ALIGN_3

.L205:
	testq	$8, N
	jle	.L206

	movsd	(X), %xmm4
	addq	INCX, X
	movhps	(X), %xmm4
	addq	INCX, X
	movsd	(Y), %xmm8
	addq	INCY, Y
	movhps	(Y), %xmm8
	addq	INCY, Y

	pshufd	$0xb1,  %xmm8, %xmm12
	mulps	%xmm4,  %xmm8
	addps	%xmm8,  %xmm0
	mulps	%xmm4,  %xmm12
	addps	%xmm12, %xmm1

	movsd	(X), %xmm5
	addq	INCX, X
	movhps	(X), %xmm5
	addq	INCX, X
	movsd	(Y), %xmm9
	addq	INCY, Y
	movhps	(Y), %xmm9
	addq	INCY, Y

	pshufd	$0xb1,  %xmm9, %xmm12
	mulps	%xmm5,  %xmm9
	addps	%xmm9,  %xmm2
	mulps	%xmm5,  %xmm12
	addps	%xmm12, %xmm3

	movsd	(X), %xmm6
	addq	INCX, X
	movhps	(X), %xmm6
	addq	INCX, X
	movsd	(Y), %xmm10
	addq	INCY, Y
	movhps	(Y), %xmm10
	addq	INCY, Y

	pshufd	$0xb1,  %xmm10, %xmm12
	mulps	%xmm6,  %xmm10
	addps	%xmm10, %xmm0
	mulps	%xmm6,  %xmm12
	addps	%xmm12, %xmm1

	movsd	(X), %xmm7
	addq	INCX, X
	movhps	(X), %xmm7
	addq	INCX, X
	movsd	(Y), %xmm11
	addq	INCY, Y
	movhps	(Y), %xmm11
	addq	INCY, Y

	pshufd	$0xb1,  %xmm11, %xmm12
	mulps	%xmm7,  %xmm11
	addps	%xmm11, %xmm2
	mulps	%xmm7,  %xmm12
	addps	%xmm12, %xmm3
	ALIGN_3

.L206:
	testq	$4, N
	jle	.L207

	movsd	(X), %xmm4
	addq	INCX, X
	movhps	(X), %xmm4
	addq	INCX, X
	movsd	(Y), %xmm8
	addq	INCY, Y
	movhps	(Y), %xmm8
	addq	INCY, Y

	pshufd	$0xb1,  %xmm8, %xmm12
	mulps	%xmm4,  %xmm8
	addps	%xmm8,  %xmm0
	mulps	%xmm4,  %xmm12
	addps	%xmm12, %xmm1

	movsd	(X), %xmm5
	addq	INCX, X
	movhps	(X), %xmm5
	addq	INCX, X
	movsd	(Y), %xmm9
	addq	INCY, Y
	movhps	(Y), %xmm9
	addq	INCY, Y

	pshufd	$0xb1,  %xmm9, %xmm12
	mulps	%xmm5,  %xmm9
	addps	%xmm9,  %xmm2
	mulps	%xmm5,  %xmm12
	addps	%xmm12, %xmm3
	ALIGN_3

.L207:
	testq	$2, N
	jle	.L208

	movsd	(X), %xmm4
	addq	INCX, X
	movhps	(X), %xmm4
	addq	INCX, X
	movsd	(Y), %xmm8
	addq	INCY, Y
	movhps	(Y), %xmm8
	addq	INCY, Y

	pshufd	$0xb1,  %xmm8, %xmm12
	mulps	%xmm4,  %xmm8
	addps	%xmm8,  %xmm0
	mulps	%xmm4,  %xmm12
	addps	%xmm12, %xmm1
	ALIGN_3

.L208:
	testq	$1, N
	jle	.L98

#ifdef movsd
	xorps	%xmm4, %xmm4
#endif
	movsd	(X), %xmm4
#ifdef movsd
	xorps	%xmm8, %xmm8
#endif
	movsd	(Y), %xmm8

	pshufd	$0xb1,  %xmm8, %xmm12
	mulps	%xmm4,  %xmm8
	addps	%xmm8,  %xmm0
	mulps	%xmm4,  %xmm12
	addps	%xmm12, %xmm1
	ALIGN_3

.L98:
	addps	%xmm2, %xmm0
	addps	%xmm3, %xmm1

	movhlps	%xmm0, %xmm2
	movhlps	%xmm1, %xmm3

	addps	%xmm2, %xmm0
	addps	%xmm3, %xmm1

	pshufd	$1, %xmm0, %xmm2
	pshufd	$1, %xmm1, %xmm3
	ALIGN_3

.L999:
#ifndef CONJ
	subss	 %xmm2, %xmm0
	addss	 %xmm3, %xmm1
#else
	addss	 %xmm2, %xmm0
	subss	 %xmm3, %xmm1
#endif
	unpcklps %xmm1, %xmm0

#ifdef WINDOWS_ABI
	movq	%xmm0, %rax
#endif

	RESTOREREGISTERS

	ret
	ALIGN_3

	EPILOGUE
