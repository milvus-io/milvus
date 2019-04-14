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

	leaq	(, INCX, SIZE), INCX
	leaq	(, INCY, SIZE), INCY

	xorps	%xmm0, %xmm0
	xorps	%xmm1, %xmm1
	xorps	%xmm2, %xmm2
	xorps	%xmm3, %xmm3

	cmpq	$0, N
	jle	.L999

	cmpq	$SIZE, INCX
	jne	.L50
	cmpq	$SIZE, INCY
	jne	.L50

	subq	$-32 * SIZE, X
	subq	$-32 * SIZE, Y

	cmpq	$3, N
	jle	.L17

	testq	$SIZE, Y
	je	.L05

	movss	-32 * SIZE(X), %xmm0
	mulss	-32 * SIZE(Y), %xmm0
	addq	$1 * SIZE, X
	addq	$1 * SIZE, Y
	decq	N
	ALIGN_2

.L05:
	testq	$2 * SIZE, Y
	je	.L10

#ifdef movsd
	xorps	%xmm4, %xmm4
#endif
	movsd	-32 * SIZE(X), %xmm4
#ifdef movsd
	xorps	%xmm1, %xmm1
#endif
	movsd	-32 * SIZE(Y), %xmm1
	mulps	%xmm4, %xmm1
	addq	$2 * SIZE, X
	addq	$2 * SIZE, Y
	subq	$2, N
	jle	.L999
	ALIGN_2

.L10:
#ifdef ALIGNED_ACCESS
	testq	$2 * SIZE, X
	jne	.L30

	testq	$SIZE, X
	jne	.L20
#else
	testq	$3 * SIZE, X
	jne	.L20
#endif

	movq	N,  %rax
	sarq	$5, %rax
	jle	.L14

	movaps	-32 * SIZE(X), %xmm4
	movaps	-28 * SIZE(X), %xmm5
	movaps	-24 * SIZE(X), %xmm6
	movaps	-20 * SIZE(X), %xmm7

	movaps	-16 * SIZE(X), %xmm8
	movaps	-12 * SIZE(X), %xmm9
	movaps	 -8 * SIZE(X), %xmm10
	movaps	 -4 * SIZE(X), %xmm11

	decq	%rax
	jle	.L12

	ALIGN_3

.L11:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	mulps	-32 * SIZE(Y), %xmm4
	addps	%xmm4,  %xmm0
	movaps	  0 * SIZE(X), %xmm4

	mulps	-28 * SIZE(Y), %xmm5
	addps	%xmm5,  %xmm1
	movaps	  4 * SIZE(X), %xmm5

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	mulps	-24 * SIZE(Y), %xmm6
	addps	%xmm6,  %xmm2
	movaps	  8 * SIZE(X), %xmm6

	mulps	-20 * SIZE(Y), %xmm7
	addps	%xmm7,  %xmm3
 	movaps	 12 * SIZE(X), %xmm7

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	mulps	-16 * SIZE(Y), %xmm8
	addps	%xmm8,  %xmm0
	movaps	 16 * SIZE(X), %xmm8

	mulps	-12 * SIZE(Y), %xmm9
	addps	%xmm9,  %xmm1
	movaps	 20 * SIZE(X), %xmm9

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	mulps	 -8 * SIZE(Y), %xmm10
	addps	%xmm10,  %xmm2
	movaps	 24 * SIZE(X), %xmm10

	mulps	 -4 * SIZE(Y), %xmm11
	addps	%xmm11,  %xmm3
	movaps	 28 * SIZE(X), %xmm11

	subq	$-32 * SIZE, X
	subq	$-32 * SIZE, Y

	decq	%rax
	jg	.L11
	ALIGN_3

.L12:
	mulps	-32 * SIZE(Y), %xmm4
	addps	%xmm4,  %xmm0
	mulps	-28 * SIZE(Y), %xmm5
	addps	%xmm5,  %xmm1
	mulps	-24 * SIZE(Y), %xmm6
	addps	%xmm6,  %xmm2
	mulps	-20 * SIZE(Y), %xmm7
	addps	%xmm7,  %xmm3

	mulps	-16 * SIZE(Y), %xmm8
	addps	%xmm8,  %xmm0
	mulps	-12 * SIZE(Y), %xmm9
	addps	%xmm9,  %xmm1
	mulps	 -8 * SIZE(Y), %xmm10
	addps	%xmm10,  %xmm2
	mulps	 -4 * SIZE(Y), %xmm11
	addps	%xmm11,  %xmm3

	subq	$-32 * SIZE, X
	subq	$-32 * SIZE, Y
	ALIGN_3

.L14:
	testq	$31, N
	jle	.L999

	testq	$16, N
	jle	.L15

	movaps	-32 * SIZE(X), %xmm4
	movaps	-28 * SIZE(X), %xmm5
	movaps	-24 * SIZE(X), %xmm6
	movaps	-20 * SIZE(X), %xmm7

	mulps	-32 * SIZE(Y), %xmm4
	addps	%xmm4,  %xmm0
	mulps	-28 * SIZE(Y), %xmm5
	addps	%xmm5,  %xmm1
	mulps	-24 * SIZE(Y), %xmm6
	addps	%xmm6,  %xmm2
	mulps	-20 * SIZE(Y), %xmm7
	addps	%xmm7,  %xmm3

	addq	$16 * SIZE, X
	addq	$16 * SIZE, Y
	ALIGN_3

.L15:
	testq	$8, N
	jle	.L16

	movaps	-32 * SIZE(X), %xmm4
	movaps	-28 * SIZE(X), %xmm5

	mulps	-32 * SIZE(Y), %xmm4
	addps	%xmm4,  %xmm0
	mulps	-28 * SIZE(Y), %xmm5
	addps	%xmm5,  %xmm1

	addq	$8 * SIZE, X
	addq	$8 * SIZE, Y
	ALIGN_3

.L16:
	testq	$4, N
	jle	.L17

	movaps	-32 * SIZE(X), %xmm4
	mulps	-32 * SIZE(Y), %xmm4

	addps	%xmm4,  %xmm2

	addq	$4 * SIZE, X
	addq	$4 * SIZE, Y
	ALIGN_3

.L17:
	testq	$2, N
	jle	.L18

#ifdef movsd
	xorps	%xmm4, %xmm4
#endif
	movsd	-32 * SIZE(X), %xmm4
#ifdef movsd
	xorps	%xmm8, %xmm8
#endif
	movsd	-32 * SIZE(Y), %xmm8

	mulps	%xmm8,  %xmm4
	addps	%xmm4,  %xmm3

	addq	$2 * SIZE, X
	addq	$2 * SIZE, Y
	ALIGN_3

.L18:
	testq	$1, N
	jle	.L999

	movss	-32 * SIZE(X), %xmm4
	mulss	-32 * SIZE(Y), %xmm4
	addss	%xmm4,  %xmm0
	jmp	.L999
	ALIGN_3

.L20:
#ifdef ALIGNED_ACCESS

	movaps	-33 * SIZE(X), %xmm4
	addq	$3 * SIZE, X

	movq	N,  %rax
	sarq	$5, %rax
	jle	.L24

	movaps	-32 * SIZE(X), %xmm5
	movaps	-28 * SIZE(X), %xmm6
	movaps	-24 * SIZE(X), %xmm7

	movaps	-20 * SIZE(X), %xmm8
	movaps	-16 * SIZE(X), %xmm9
	movaps	-12 * SIZE(X), %xmm10
	movaps	 -8 * SIZE(X), %xmm11

	decq	%rax
	jle	.L22

	ALIGN_3

.L21:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movss	%xmm5, %xmm4
	pshufd	$0x39, %xmm4,  %xmm4
	mulps	-32 * SIZE(Y), %xmm4
	addps	%xmm4,  %xmm0
	movaps	 -4 * SIZE(X), %xmm4

	movss	%xmm6, %xmm5
	pshufd	$0x39, %xmm5,  %xmm5
	mulps	-28 * SIZE(Y), %xmm5
	addps	%xmm5,  %xmm1
	movaps	  0 * SIZE(X), %xmm5

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	movss	%xmm7, %xmm6
	pshufd	$0x39, %xmm6,  %xmm6
	mulps	-24 * SIZE(Y), %xmm6
	addps	%xmm6,  %xmm2
	movaps	  4 * SIZE(X), %xmm6

	movss	%xmm8, %xmm7
	pshufd	$0x39, %xmm7,  %xmm7
	mulps	-20 * SIZE(Y), %xmm7
	addps	%xmm7,  %xmm3
 	movaps	  8 * SIZE(X), %xmm7

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	movss	%xmm9, %xmm8
	pshufd	$0x39, %xmm8,  %xmm8
	mulps	-16 * SIZE(Y), %xmm8
	addps	%xmm8,  %xmm0
	movaps	 12 * SIZE(X), %xmm8

	movss	%xmm10, %xmm9
	pshufd	$0x39, %xmm9,  %xmm9
	mulps	-12 * SIZE(Y), %xmm9
	addps	%xmm9,  %xmm1
	movaps	 16 * SIZE(X), %xmm9

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	movss	%xmm11, %xmm10
	pshufd	$0x39, %xmm10, %xmm10
	mulps	 -8 * SIZE(Y), %xmm10
	addps	%xmm10,  %xmm2
	movaps	 20 * SIZE(X), %xmm10

	movss	%xmm4, %xmm11
	pshufd	$0x39, %xmm11, %xmm11
	mulps	 -4 * SIZE(Y), %xmm11
	addps	%xmm11,  %xmm3
	movaps	 24 * SIZE(X), %xmm11

	subq	$-32 * SIZE, X
	subq	$-32 * SIZE, Y

	decq	%rax
	jg	.L21
	ALIGN_3

.L22:
	movss	%xmm5, %xmm4
	pshufd	$0x39, %xmm4,  %xmm4
	mulps	-32 * SIZE(Y), %xmm4
	addps	%xmm4,  %xmm0
	movaps	 -4 * SIZE(X), %xmm4

	movss	%xmm6, %xmm5
	pshufd	$0x39, %xmm5,  %xmm5
	mulps	-28 * SIZE(Y), %xmm5
	addps	%xmm5,  %xmm1

	movss	%xmm7, %xmm6
	pshufd	$0x39, %xmm6,  %xmm6
	mulps	-24 * SIZE(Y), %xmm6
	addps	%xmm6,  %xmm2

	movss	%xmm8, %xmm7
	pshufd	$0x39, %xmm7,  %xmm7
	mulps	-20 * SIZE(Y), %xmm7
	addps	%xmm7,  %xmm3

	movss	%xmm9, %xmm8
	pshufd	$0x39, %xmm8,  %xmm8
	mulps	-16 * SIZE(Y), %xmm8
	addps	%xmm8,  %xmm0

	movss	%xmm10, %xmm9
	pshufd	$0x39, %xmm9,  %xmm9
	mulps	-12 * SIZE(Y), %xmm9
	addps	%xmm9,  %xmm1

	movss	%xmm11, %xmm10
	pshufd	$0x39, %xmm10, %xmm10
	mulps	 -8 * SIZE(Y), %xmm10
	addps	%xmm10,  %xmm2

	movss	%xmm4, %xmm11
	pshufd	$0x39, %xmm11, %xmm11
	mulps	 -4 * SIZE(Y), %xmm11
	addps	%xmm11,  %xmm3

	subq	$-32 * SIZE, X
	subq	$-32 * SIZE, Y
	ALIGN_3

.L24:
	testq	$31, N
	jle	.L999

	testq	$16, N
	jle	.L25

	movaps	-32 * SIZE(X), %xmm5
	movaps	-28 * SIZE(X), %xmm6
	movaps	-24 * SIZE(X), %xmm7

	movss	%xmm5, %xmm4
	pshufd	$0x39, %xmm4,  %xmm4
	mulps	-32 * SIZE(Y), %xmm4
	addps	%xmm4,  %xmm0
	movaps	-20 * SIZE(X), %xmm4

	movss	%xmm6, %xmm5
	pshufd	$0x39, %xmm5,  %xmm5
	mulps	-28 * SIZE(Y), %xmm5
	addps	%xmm5,  %xmm1

	movss	%xmm7, %xmm6
	pshufd	$0x39, %xmm6,  %xmm6
	mulps	-24 * SIZE(Y), %xmm6
	addps	%xmm6,  %xmm2

	movss	%xmm4, %xmm7
	pshufd	$0x39, %xmm7,  %xmm7
	mulps	-20 * SIZE(Y), %xmm7
	addps	%xmm7,  %xmm3

	addq	$16 * SIZE, X
	addq	$16 * SIZE, Y
	ALIGN_3

.L25:
	testq	$8, N
	jle	.L26

	movaps	-32 * SIZE(X), %xmm5
	movaps	-28 * SIZE(X), %xmm6

	movss	%xmm5, %xmm4
	pshufd	$0x39, %xmm4,  %xmm4
	mulps	-32 * SIZE(Y), %xmm4
	addps	%xmm4, %xmm0

	movss	%xmm6, %xmm5
	pshufd	$0x39, %xmm5,  %xmm5
	mulps	-28 * SIZE(Y), %xmm5
	addps	%xmm5, %xmm1

	movaps	%xmm6, %xmm4

	addq	$8 * SIZE, X
	addq	$8 * SIZE, Y
	ALIGN_3

.L26:
	testq	$4, N
	jle	.L27

	movaps	-32 * SIZE(X), %xmm5
	movss	%xmm5, %xmm4
	pshufd	$0x39, %xmm4,  %xmm4
	mulps	-32 * SIZE(Y), %xmm4
	addps	%xmm4, %xmm2
	movaps	%xmm5, %xmm4

	addq	$4 * SIZE, X
	addq	$4 * SIZE, Y
	ALIGN_3

.L27:
	testq	$2, N
	jle	.L28

#ifdef movsd
	xorps	%xmm8, %xmm8
#endif
	movsd	-32 * SIZE(Y), %xmm8

	pshufd	$0x29, %xmm4,  %xmm5

	mulps	%xmm8,  %xmm5
	addps	%xmm5,  %xmm3
	movhlps	%xmm4,  %xmm4

	addq	$2 * SIZE, X
	addq	$2 * SIZE, Y
	ALIGN_3

.L28:
	testq	$1, N
	jle	.L999

	pshufd	$0x39, %xmm4,  %xmm4
	mulss	-32 * SIZE(Y), %xmm4
	addss	%xmm4,  %xmm0
	jmp	.L999
	ALIGN_3

.L30:
	testq	$SIZE, X
	jne	.L40

	movhps	-32 * SIZE(X), %xmm4
	addq	$2 * SIZE, X

	movq	N,  %rax
	sarq	$5, %rax
	jle	.L34

	movaps	-32 * SIZE(X), %xmm5
	movaps	-28 * SIZE(X), %xmm6
	movaps	-24 * SIZE(X), %xmm7
	movaps	-20 * SIZE(X), %xmm8

	movaps	-16 * SIZE(X), %xmm9
	movaps	-12 * SIZE(X), %xmm10
	movaps	 -8 * SIZE(X), %xmm11

	decq	%rax
	jle	.L32

	ALIGN_3

.L31:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	SHUFPD_1 %xmm5, %xmm4
	mulps	-32 * SIZE(Y), %xmm4
	addps	 %xmm4,  %xmm0
	movaps	 -4 * SIZE(X), %xmm4

	SHUFPD_1 %xmm6, %xmm5
	mulps	-28 * SIZE(Y), %xmm5
	addps	 %xmm5,  %xmm1
	movaps	  0 * SIZE(X), %xmm5

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	SHUFPD_1 %xmm7, %xmm6
	mulps	-24 * SIZE(Y), %xmm6
	addps	 %xmm6,  %xmm2
	movaps	  4 * SIZE(X), %xmm6

	SHUFPD_1 %xmm8, %xmm7
	mulps	-20 * SIZE(Y), %xmm7
	addps	 %xmm7,  %xmm3
	movaps	  8 * SIZE(X), %xmm7

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	SHUFPD_1 %xmm9, %xmm8
	mulps	-16 * SIZE(Y), %xmm8
	addps	 %xmm8,  %xmm0
	movaps	 12 * SIZE(X), %xmm8

	SHUFPD_1 %xmm10, %xmm9
	mulps	-12 * SIZE(Y), %xmm9
	addps	 %xmm9,  %xmm1
	movaps	 16 * SIZE(X), %xmm9

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	SHUFPD_1 %xmm11, %xmm10
	mulps	 -8 * SIZE(Y), %xmm10
	addps	 %xmm10, %xmm2
	movaps	 20 * SIZE(X), %xmm10

	SHUFPD_1 %xmm4, %xmm11
	mulps	 -4 * SIZE(Y), %xmm11
	addps	 %xmm11, %xmm3
	movaps	 24 * SIZE(X), %xmm11

	subq	$-32 * SIZE, X
	subq	$-32 * SIZE, Y

	decq	%rax
	jg	.L31
	ALIGN_3

.L32:
	SHUFPD_1 %xmm5, %xmm4
	mulps	-32 * SIZE(Y), %xmm4
	addps	 %xmm4,  %xmm0
	movaps	 -4 * SIZE(X), %xmm4

	SHUFPD_1 %xmm6, %xmm5
	mulps	-28 * SIZE(Y), %xmm5
	addps	 %xmm5,  %xmm1

	SHUFPD_1 %xmm7, %xmm6
	mulps	-24 * SIZE(Y), %xmm6
	addps	 %xmm6,  %xmm2

	SHUFPD_1 %xmm8, %xmm7
	mulps	-20 * SIZE(Y), %xmm7
	addps	 %xmm7,  %xmm3

	SHUFPD_1 %xmm9, %xmm8
	mulps	-16 * SIZE(Y), %xmm8
	addps	 %xmm8,  %xmm0

	SHUFPD_1 %xmm10, %xmm9
	mulps	-12 * SIZE(Y), %xmm9
	addps	 %xmm9,  %xmm1

	SHUFPD_1 %xmm11, %xmm10
	mulps	 -8 * SIZE(Y), %xmm10
	addps	 %xmm10, %xmm2

	SHUFPD_1 %xmm4, %xmm11
	mulps	 -4 * SIZE(Y), %xmm11
	addps	 %xmm11, %xmm3

	subq	$-32 * SIZE, X
	subq	$-32 * SIZE, Y
	ALIGN_3

.L34:
	testq	$31, N
	jle	.L999

	testq	$16, N
	jle	.L35

	movaps	-32 * SIZE(X), %xmm5
	movaps	-28 * SIZE(X), %xmm6
	movaps	-24 * SIZE(X), %xmm7

	SHUFPD_1 %xmm5, %xmm4
	mulps	-32 * SIZE(Y), %xmm4
	addps	 %xmm4,  %xmm0
	movaps	-20 * SIZE(X), %xmm4

	SHUFPD_1 %xmm6, %xmm5
	mulps	-28 * SIZE(Y), %xmm5
	addps	 %xmm5,  %xmm1

	SHUFPD_1 %xmm7, %xmm6
	mulps	-24 * SIZE(Y), %xmm6
	addps	 %xmm6,  %xmm2

	SHUFPD_1 %xmm4, %xmm7
	mulps	-20 * SIZE(Y), %xmm7
	addps	 %xmm7,  %xmm3

	addq	$16 * SIZE, X
	addq	$16 * SIZE, Y
	ALIGN_3

.L35:
	testq	$8, N
	jle	.L36

	movaps	-32 * SIZE(X), %xmm5
	movaps	-28 * SIZE(X), %xmm6

	SHUFPD_1 %xmm5, %xmm4
	mulps	-32 * SIZE(Y), %xmm4
	addps	 %xmm4, %xmm0
	SHUFPD_1 %xmm6, %xmm5
	mulps	-28 * SIZE(Y), %xmm5
	addps	 %xmm5, %xmm1
	movapd	 %xmm6, %xmm4

	addq	$8 * SIZE, X
	addq	$8 * SIZE, Y
	ALIGN_3

.L36:
	testq	$4, N
	jle	.L37

	movaps	-32 * SIZE(X), %xmm5

	SHUFPD_1 %xmm5, %xmm4
	mulps	-32 * SIZE(Y), %xmm4
	addps	%xmm4, %xmm0
	movaps	%xmm5, %xmm4

	addq	$4 * SIZE, X
	addq	$4 * SIZE, Y
	ALIGN_3

.L37:
	testq	$2, N
	jle	.L38

	xorps	%xmm5, %xmm5
	movhlps	%xmm4, %xmm5

	movlps  -32 * SIZE(Y), %xmm4
	mulps	%xmm4, %xmm5
	addps	%xmm5, %xmm0

	addq	$2 * SIZE, X
	addq	$2 * SIZE, Y
	ALIGN_3

.L38:
	testq	$1, N
	jle	.L999

	movss	-34 * SIZE(X), %xmm4
	mulss	-32 * SIZE(Y), %xmm4
	addss	%xmm4,  %xmm0
	jmp	.L999
	ALIGN_3

.L40:
	movaps	-35 * SIZE(X), %xmm4
	addq	$SIZE, X

	movq	N,  %rax
	sarq	$5, %rax
	jle	.L44

	movaps	-32 * SIZE(X), %xmm5
	movaps	-28 * SIZE(X), %xmm6
	movaps	-24 * SIZE(X), %xmm7

	movaps	-20 * SIZE(X), %xmm8
	movaps	-16 * SIZE(X), %xmm9
	movaps	-12 * SIZE(X), %xmm10
	movaps	 -8 * SIZE(X), %xmm11

	decq	%rax
	jle	.L42

	ALIGN_3

.L41:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movss	%xmm5, %xmm4
	shufps	$0x93, %xmm5,  %xmm4
	mulps	-32 * SIZE(Y), %xmm4
	addps	%xmm4,  %xmm0
	movaps	 -4 * SIZE(X), %xmm4

	movss	%xmm6, %xmm5
	shufps	$0x93, %xmm6,  %xmm5
	mulps	-28 * SIZE(Y), %xmm5
	addps	%xmm5,  %xmm1
	movaps	  0 * SIZE(X), %xmm5

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	movss	%xmm7, %xmm6
	shufps	$0x93, %xmm7,  %xmm6
	mulps	-24 * SIZE(Y), %xmm6
	addps	%xmm6,  %xmm2
	movaps	  4 * SIZE(X), %xmm6

	movss	%xmm8, %xmm7
	shufps	$0x93, %xmm8,  %xmm7
	mulps	-20 * SIZE(Y), %xmm7
	addps	%xmm7,  %xmm3
 	movaps	  8 * SIZE(X), %xmm7

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	movss	%xmm9, %xmm8
	shufps	$0x93, %xmm9,  %xmm8
	mulps	-16 * SIZE(Y), %xmm8
	addps	%xmm8,  %xmm0
	movaps	 12 * SIZE(X), %xmm8

	movss	%xmm10, %xmm9
	shufps	$0x93, %xmm10, %xmm9
	mulps	-12 * SIZE(Y), %xmm9
	addps	%xmm9,  %xmm1
	movaps	 16 * SIZE(X), %xmm9

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	movss	%xmm11, %xmm10
	shufps	$0x93, %xmm11, %xmm10
	mulps	 -8 * SIZE(Y), %xmm10
	addps	%xmm10,  %xmm2
	movaps	 20 * SIZE(X), %xmm10

	movss	%xmm4, %xmm11
	shufps	$0x93, %xmm4,  %xmm11
	mulps	 -4 * SIZE(Y), %xmm11
	addps	%xmm11,  %xmm3
	movaps	 24 * SIZE(X), %xmm11

	subq	$-32 * SIZE, X
	subq	$-32 * SIZE, Y

	decq	%rax
	jg	.L41
	ALIGN_3

.L42:
	movss	%xmm5, %xmm4
	shufps	$0x93, %xmm5,  %xmm4
	mulps	-32 * SIZE(Y), %xmm4
	addps	%xmm4,  %xmm0
	movaps	 -4 * SIZE(X), %xmm4

	movss	%xmm6, %xmm5
	shufps	$0x93, %xmm6,  %xmm5
	mulps	-28 * SIZE(Y), %xmm5
	addps	%xmm5,  %xmm1

	movss	%xmm7, %xmm6
	shufps	$0x93, %xmm7,  %xmm6
	mulps	-24 * SIZE(Y), %xmm6
	addps	%xmm6,  %xmm2

	movss	%xmm8, %xmm7
	shufps	$0x93, %xmm8,  %xmm7
	mulps	-20 * SIZE(Y), %xmm7
	addps	%xmm7,  %xmm3

	movss	%xmm9, %xmm8
	shufps	$0x93, %xmm9,  %xmm8
	mulps	-16 * SIZE(Y), %xmm8
	addps	%xmm8,  %xmm0

	movss	%xmm10, %xmm9
	shufps	$0x93, %xmm10, %xmm9
	mulps	-12 * SIZE(Y), %xmm9
	addps	%xmm9,  %xmm1

	movss	%xmm11, %xmm10
	shufps	$0x93, %xmm11, %xmm10
	mulps	 -8 * SIZE(Y), %xmm10
	addps	%xmm10,  %xmm2

	movss	%xmm4, %xmm11
	shufps	$0x93, %xmm4,  %xmm11
	mulps	 -4 * SIZE(Y), %xmm11
	addps	%xmm11,  %xmm3

	subq	$-32 * SIZE, X
	subq	$-32 * SIZE, Y
	ALIGN_3

.L44:
	testq	$31, N
	jle	.L999

	testq	$16, N
	jle	.L45

	movaps	-32 * SIZE(X), %xmm5
	movaps	-28 * SIZE(X), %xmm6
	movaps	-24 * SIZE(X), %xmm7

	movss	%xmm5, %xmm4
	shufps	$0x93, %xmm5,  %xmm4
	mulps	-32 * SIZE(Y), %xmm4
	addps	%xmm4,  %xmm0
	movaps	-20 * SIZE(X), %xmm4

	movss	%xmm6, %xmm5
	shufps	$0x93, %xmm6,  %xmm5
	mulps	-28 * SIZE(Y), %xmm5
	addps	%xmm5,  %xmm1

	movss	%xmm7, %xmm6
	shufps	$0x93, %xmm7,  %xmm6
	mulps	-24 * SIZE(Y), %xmm6
	addps	%xmm6,  %xmm2

	movss	%xmm4, %xmm7
	shufps	$0x93, %xmm4,  %xmm7
	mulps	-20 * SIZE(Y), %xmm7
	addps	%xmm7,  %xmm3

	addq	$16 * SIZE, X
	addq	$16 * SIZE, Y
	ALIGN_3

.L45:
	testq	$8, N
	jle	.L46

	movaps	-32 * SIZE(X), %xmm5
	movaps	-28 * SIZE(X), %xmm6

	movss	%xmm5, %xmm4
	shufps	$0x93, %xmm5,  %xmm4
	mulps	-32 * SIZE(Y), %xmm4
	addps	%xmm4, %xmm0

	movss	%xmm6, %xmm5
	shufps	$0x93, %xmm6,  %xmm5
	mulps	-28 * SIZE(Y), %xmm5
	addps	%xmm5, %xmm1

	movaps	%xmm6, %xmm4

	addq	$8 * SIZE, X
	addq	$8 * SIZE, Y
	ALIGN_3

.L46:
	testq	$4, N
	jle	.L47

	movaps	-32 * SIZE(X), %xmm5
	movss	%xmm5, %xmm4
	shufps	$0x93, %xmm5,  %xmm4
	mulps	-32 * SIZE(Y), %xmm4
	addps	%xmm4, %xmm2
	movaps	%xmm5, %xmm4

	addq	$4 * SIZE, X
	addq	$4 * SIZE, Y
	ALIGN_3

.L47:
	testq	$2, N
	jle	.L48

	movaps	-32 * SIZE(X), %xmm5
#ifdef movsd
	xorps	%xmm8, %xmm8
#endif
	movsd	-32 * SIZE(Y), %xmm8

	movss	%xmm5, %xmm4
	shufps	$0x93, %xmm4,  %xmm4

	mulps	%xmm8,  %xmm4
	addps	%xmm4,  %xmm3
	movlhps	%xmm5,  %xmm4

	addq	$2 * SIZE, X
	addq	$2 * SIZE, Y
	ALIGN_3

.L48:
	testq	$1, N
	jle	.L999

	pshufd	$0x93, %xmm4,  %xmm4
	mulss	-32 * SIZE(Y), %xmm4
	addss	%xmm4,  %xmm0
	jmp	.L999
	ALIGN_4

#else
	movq	N,  %rax
	sarq	$5, %rax
	jle	.L24

	movlps	-32 * SIZE(X), %xmm4
	movhps	-30 * SIZE(X), %xmm4
	movlps	-28 * SIZE(X), %xmm5
	movhps	-26 * SIZE(X), %xmm5
	movlps	-24 * SIZE(X), %xmm6
	movhps	-22 * SIZE(X), %xmm6
	movlps	-20 * SIZE(X), %xmm7
	movhps	-18 * SIZE(X), %xmm7

	movlps	-16 * SIZE(X), %xmm8
	movhps	-14 * SIZE(X), %xmm8
	movlps	-12 * SIZE(X), %xmm9
	movhps	-10 * SIZE(X), %xmm9
	movlps	 -8 * SIZE(X), %xmm10
	movhps	 -6 * SIZE(X), %xmm10
	movlps	 -4 * SIZE(X), %xmm11
	movhps	 -2 * SIZE(X), %xmm11

	decq	%rax
	jle	.L22

	ALIGN_3

.L21:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	mulps	-32 * SIZE(Y), %xmm4
	addps	%xmm4,  %xmm0
	movlps	  0 * SIZE(X), %xmm4
	movhps	  2 * SIZE(X), %xmm4

	mulps	-28 * SIZE(Y), %xmm5
	addps	%xmm5,  %xmm1
	movlps	  4 * SIZE(X), %xmm5
	movhps	  6 * SIZE(X), %xmm5

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	mulps	-24 * SIZE(Y), %xmm6
	addps	%xmm6,  %xmm2
	movlps	  8 * SIZE(X), %xmm6
	movhps	 10 * SIZE(X), %xmm6

	mulps	-20 * SIZE(Y), %xmm7
	addps	%xmm7,  %xmm3
	movlps	 12 * SIZE(X), %xmm7
	movhps	 14 * SIZE(X), %xmm7

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	mulps	-16 * SIZE(Y), %xmm8
	addps	%xmm8,  %xmm0
	movlps	 16 * SIZE(X), %xmm8
	movhps	 18 * SIZE(X), %xmm8

	mulps	-12 * SIZE(Y), %xmm9
	addps	%xmm9,  %xmm1
	movlps	 20 * SIZE(X), %xmm9
	movhps	 22 * SIZE(X), %xmm9

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	mulps	 -8 * SIZE(Y), %xmm10
	addps	%xmm10,  %xmm2
	movlps	 24 * SIZE(X), %xmm10
	movhps	 26 * SIZE(X), %xmm10

	mulps	 -4 * SIZE(Y), %xmm11
	addps	%xmm11,  %xmm3
	movlps	 28 * SIZE(X), %xmm11
	movhps	 30 * SIZE(X), %xmm11

	subq	$-32 * SIZE, X
	subq	$-32 * SIZE, Y

	decq	%rax
	jg	.L21
	ALIGN_3

.L22:
	mulps	-32 * SIZE(Y), %xmm4
	addps	%xmm4,  %xmm0
	mulps	-28 * SIZE(Y), %xmm5
	addps	%xmm5,  %xmm1
	mulps	-24 * SIZE(Y), %xmm6
	addps	%xmm6,  %xmm2
	mulps	-20 * SIZE(Y), %xmm7
	addps	%xmm7,  %xmm3

	mulps	-16 * SIZE(Y), %xmm8
	addps	%xmm8,  %xmm0
	mulps	-12 * SIZE(Y), %xmm9
	addps	%xmm9,  %xmm1
	mulps	 -8 * SIZE(Y), %xmm10
	addps	%xmm10,  %xmm2
	mulps	 -4 * SIZE(Y), %xmm11
	addps	%xmm11,  %xmm3

	subq	$-32 * SIZE, X
	subq	$-32 * SIZE, Y
	ALIGN_3

.L24:
	testq	$31, N
	jle	.L999

	testq	$16, N
	jle	.L25

	movlps	-32 * SIZE(X), %xmm4
	movhps	-30 * SIZE(X), %xmm4
	movlps	-28 * SIZE(X), %xmm5
	movhps	-26 * SIZE(X), %xmm5
	movlps	-24 * SIZE(X), %xmm6
	movhps	-22 * SIZE(X), %xmm6
	movlps	-20 * SIZE(X), %xmm7
	movhps	-18 * SIZE(X), %xmm7

	mulps	-32 * SIZE(Y), %xmm4
	addps	%xmm4,  %xmm0
	mulps	-28 * SIZE(Y), %xmm5
	addps	%xmm5,  %xmm1
	mulps	-24 * SIZE(Y), %xmm6
	addps	%xmm6,  %xmm2
	mulps	-20 * SIZE(Y), %xmm7
	addps	%xmm7,  %xmm3

	addq	$16 * SIZE, X
	addq	$16 * SIZE, Y
	ALIGN_3

.L25:
	testq	$8, N
	jle	.L26

	movlps	-32 * SIZE(X), %xmm4
	movhps	-30 * SIZE(X), %xmm4
	movlps	-28 * SIZE(X), %xmm5
	movhps	-26 * SIZE(X), %xmm5

	mulps	-32 * SIZE(Y), %xmm4
	addps	%xmm4,  %xmm0
	mulps	-28 * SIZE(Y), %xmm5
	addps	%xmm5,  %xmm1

	addq	$8 * SIZE, X
	addq	$8 * SIZE, Y
	ALIGN_3

.L26:
	testq	$4, N
	jle	.L27

	movlps	-32 * SIZE(X), %xmm4
	movhps	-30 * SIZE(X), %xmm4
	mulps	-32 * SIZE(Y), %xmm4

	addps	%xmm4,  %xmm2

	addq	$4 * SIZE, X
	addq	$4 * SIZE, Y
	ALIGN_3

.L27:
	testq	$2, N
	jle	.L28

#ifdef movsd
	xorps	%xmm4, %xmm4
#endif
	movsd	-32 * SIZE(X), %xmm4
#ifdef movsd
	xorps	%xmm8, %xmm8
#endif
	movsd	-32 * SIZE(Y), %xmm8

	mulps	%xmm8,  %xmm4
	addps	%xmm4,  %xmm3

	addq	$2 * SIZE, X
	addq	$2 * SIZE, Y
	ALIGN_3

.L28:
	testq	$1, N
	jle	.L999

	movss	-32 * SIZE(X), %xmm4
	mulss	-32 * SIZE(Y), %xmm4
	addss	%xmm4,  %xmm0
	jmp	.L999
	ALIGN_3
#endif


.L50:
	movq	N,  %rax
	sarq	$2, %rax
	jle	.L55
	ALIGN_3

.L53:
	movss	0 * SIZE(X), %xmm4
	addq	INCX, X
	mulss	0 * SIZE(Y), %xmm4
	addq	INCY, Y
	movss	0 * SIZE(X), %xmm5
	addq	INCX, X
	mulss	0 * SIZE(Y), %xmm5
	addq	INCY, Y
	movss	0 * SIZE(X), %xmm6
	addq	INCX, X
	mulss	0 * SIZE(Y), %xmm6
	addq	INCY, Y
	movss	0 * SIZE(X), %xmm7
	addq	INCX, X
	mulss	0 * SIZE(Y), %xmm7
	addq	INCY, Y

	addss	%xmm4, %xmm0
	addss	%xmm5, %xmm1
	addss	%xmm6, %xmm2
	addss	%xmm7, %xmm3

	decq	%rax
	jg	.L53
	ALIGN_3

.L55:
	movq	N, %rax
	andq	$3,   %rax
	jle	.L999
	ALIGN_3

.L56:
	movss	0 * SIZE(X), %xmm4
	addq	INCX, X
	mulss	0 * SIZE(Y), %xmm4
	addq	INCY, Y
	addss	%xmm4, %xmm0
	decq	%rax
	jg	.L56
	ALIGN_3

.L999:
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

#ifdef DSDOT
	cvtss2sd %xmm0, %xmm0
#endif

	RESTOREREGISTERS

	ret

	EPILOGUE
