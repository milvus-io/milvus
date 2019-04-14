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
#define Y	ARG6
#define INCY	ARG2
#else
#define M	ARG1
#define X	ARG2
#define INCX	ARG3
#define Y	ARG4
#define INCY	%r10
#endif

#define	YY	%r11
#define ALPHA_R	%xmm14
#define ALPHA_I	%xmm15

#define  USE_PSHUFD

#if defined(HAVE_SSE3) && !defined(CORE_OPTERON)
#define MOVDDUP(a, b, c)	movddup	a(b), c
#define MOVDDUP2(a, b, c)	movddup	a##b, c
#else
#define MOVDDUP(a, b, c)	movlpd	a(b), c;movhpd	a(b), c
#define MOVDDUP2(a, b, c)	movlpd	a##b, c;movhpd	a##b, c
#endif

#include "l1param.h"

	PROLOGUE
	PROFCODE

#ifndef WINDOWS_ABI
	movq	 8(%rsp), INCY
#else
	movaps	%xmm3, %xmm0
	movsd	40(%rsp), %xmm1

	movq	48(%rsp), X
	movq	56(%rsp), INCX
	movq	64(%rsp), Y
	movq	72(%rsp), INCY
#endif

	SAVEREGISTERS

	salq	$ZBASE_SHIFT, INCX
	salq	$ZBASE_SHIFT, INCY

	testq	M, M
	jle	.L999

	cmpq	$2 * SIZE, INCX
	jne	.L50
	cmpq	$2 * SIZE, INCY
	jne	.L50

	subq	$-16 * SIZE, X
	subq	$-16 * SIZE, Y

	pcmpeqb	%xmm7, %xmm7
	psllq	$63,   %xmm7

#ifdef HAVE_SSE3
	movddup	%xmm0, ALPHA_R
	movddup	%xmm1, ALPHA_I
#else
	pshufd	$0x44, %xmm0, ALPHA_R
	pshufd	$0x44, %xmm1, ALPHA_I
#endif

#ifndef CONJ
	shufps	$0x0c, %xmm7, %xmm7
	xorpd	%xmm7, ALPHA_I
#else
	shufps	$0xc0, %xmm7, %xmm7
	xorpd	%xmm7, ALPHA_R
#endif

	testq	$SIZE, Y
	jne	.L30

	testq	$SIZE, X
	jne	.L20

	movq	M,  %rax
	sarq	$3, %rax
	jle	.L15

	movaps	-16 * SIZE(X), %xmm0
	movaps	-14 * SIZE(X), %xmm1
	movaps	-12 * SIZE(X), %xmm2
	movaps	-10 * SIZE(X), %xmm3

	decq	%rax
	jle	.L12
	ALIGN_3

.L11:
	movaps	 -8 * SIZE(X), %xmm4
	movaps	 -6 * SIZE(X), %xmm5

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

#if defined(USE_PSHUFD) || defined(USE_PSHUFD_HALF)
	pshufd	$0x4e,   %xmm0, %xmm8
#else
	movsd	-15 * SIZE(X), %xmm8
	movhps	-16 * SIZE(X), %xmm8
#endif
	mulpd	ALPHA_R, %xmm0
	mulpd	ALPHA_I, %xmm8
	addpd	-16 * SIZE(Y), %xmm0
	addpd	%xmm8,   %xmm0
	movaps	%xmm0, -16 * SIZE(Y)

#ifdef USE_PSHUFD
	pshufd	$0x4e,   %xmm1, %xmm8
#else
	movsd	-13 * SIZE(X), %xmm8
	movhps	-14 * SIZE(X), %xmm8
#endif
	mulpd	ALPHA_R, %xmm1
	mulpd	ALPHA_I, %xmm8
	addpd	-14 * SIZE(Y), %xmm1
	addpd	%xmm8,   %xmm1
	movaps	%xmm1, -14 * SIZE(Y)

	movaps	 -4 * SIZE(X), %xmm6
	movaps	 -2 * SIZE(X), %xmm7

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

#if defined(USE_PSHUFD) || defined(USE_PSHUFD_HALF)
	pshufd	$0x4e,   %xmm2, %xmm8
#else
	movsd	-11 * SIZE(X), %xmm8
	movhps	-12 * SIZE(X), %xmm8
#endif
	mulpd	ALPHA_R, %xmm2
	mulpd	ALPHA_I, %xmm8
	addpd	-12 * SIZE(Y), %xmm2
	addpd	%xmm8,   %xmm2
	movaps	%xmm2, -12 * SIZE(Y)

#ifdef USE_PSHUFD
	pshufd	$0x4e,   %xmm3, %xmm8
#else
	movsd	 -9 * SIZE(X), %xmm8
	movhps	-10 * SIZE(X), %xmm8
#endif
	mulpd	ALPHA_R, %xmm3
	mulpd	ALPHA_I, %xmm8
	addpd	-10 * SIZE(Y), %xmm3
	addpd	%xmm8,   %xmm3
	movaps	%xmm3, -10 * SIZE(Y)

	movaps	  0 * SIZE(X), %xmm0
	movaps	  2 * SIZE(X), %xmm1

#if defined(PREFETCHW) && !defined(FETCH128)
	PREFETCHW (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

#if defined(USE_PSHUFD) || defined(USE_PSHUFD_HALF)
	pshufd	$0x4e,   %xmm4, %xmm8
#else
	movsd	 -7 * SIZE(X), %xmm8
	movhps	 -8 * SIZE(X), %xmm8
#endif
	mulpd	ALPHA_R, %xmm4
	mulpd	ALPHA_I, %xmm8
	addpd	 -8 * SIZE(Y), %xmm4
	addpd	%xmm8,   %xmm4
	movaps	%xmm4,  -8 * SIZE(Y)

#ifdef USE_PSHUFD
	pshufd	$0x4e,   %xmm5, %xmm8
#else
	movsd	 -5 * SIZE(X), %xmm8
	movhps	 -6 * SIZE(X), %xmm8
#endif
	mulpd	ALPHA_R, %xmm5
	mulpd	ALPHA_I, %xmm8
	addpd	 -6 * SIZE(Y), %xmm5
	addpd	%xmm8,   %xmm5
	movaps	%xmm5,  -6 * SIZE(Y)

	movaps	  4 * SIZE(X), %xmm2
	movaps	  6 * SIZE(X), %xmm3

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

#if defined(USE_PSHUFD) || defined(USE_PSHUFD_HALF)
	pshufd	$0x4e,   %xmm6, %xmm8
#else
	movsd	 -3 * SIZE(X), %xmm8
	movhps	 -4 * SIZE(X), %xmm8
#endif
	mulpd	ALPHA_R, %xmm6
	mulpd	ALPHA_I, %xmm8
	addpd	 -4 * SIZE(Y), %xmm6
	addpd	%xmm8,   %xmm6
	movaps	%xmm6,  -4 * SIZE(Y)

#ifdef USE_PSHUFD
	pshufd	$0x4e,   %xmm7, %xmm8
#else
	movsd	 -1 * SIZE(X), %xmm8
	movhps	 -2 * SIZE(X), %xmm8
#endif
	mulpd	ALPHA_R, %xmm7
	mulpd	ALPHA_I, %xmm8
	addpd	 -2 * SIZE(Y), %xmm7
	addpd	%xmm8,   %xmm7
	movaps	%xmm7,  -2 * SIZE(Y)

	subq	$-16 * SIZE, X
	subq	$-16 * SIZE, Y
	decq	%rax
	jg	.L11
	ALIGN_3

.L12:
	movaps	 -8 * SIZE(X), %xmm4
	movaps	 -6 * SIZE(X), %xmm5

	pshufd	$0x4e,   %xmm0, %xmm8
	mulpd	ALPHA_R, %xmm0
	mulpd	ALPHA_I, %xmm8
	addpd	-16 * SIZE(Y), %xmm0
	addpd	%xmm8,   %xmm0
	movaps	%xmm0, -16 * SIZE(Y)

	pshufd	$0x4e,   %xmm1, %xmm8
	mulpd	ALPHA_R, %xmm1
	mulpd	ALPHA_I, %xmm8
	addpd	-14 * SIZE(Y), %xmm1
	addpd	%xmm8,   %xmm1
	movaps	%xmm1, -14 * SIZE(Y)

	movaps	 -4 * SIZE(X), %xmm6
	movaps	 -2 * SIZE(X), %xmm7

	pshufd	$0x4e,   %xmm2, %xmm8
	mulpd	ALPHA_R, %xmm2
	mulpd	ALPHA_I, %xmm8
	addpd	-12 * SIZE(Y), %xmm2
	addpd	%xmm8,   %xmm2
	movaps	%xmm2, -12 * SIZE(Y)

	pshufd	$0x4e,   %xmm3, %xmm8
	mulpd	ALPHA_R, %xmm3
	mulpd	ALPHA_I, %xmm8
	addpd	-10 * SIZE(Y), %xmm3
	addpd	%xmm8,   %xmm3
	movaps	%xmm3, -10 * SIZE(Y)

	pshufd	$0x4e,   %xmm4, %xmm8
	mulpd	ALPHA_R, %xmm4
	mulpd	ALPHA_I, %xmm8
	addpd	 -8 * SIZE(Y), %xmm4
	addpd	%xmm8,   %xmm4
	movaps	%xmm4,  -8 * SIZE(Y)

	pshufd	$0x4e,   %xmm5, %xmm8
	mulpd	ALPHA_R, %xmm5
	mulpd	ALPHA_I, %xmm8
	addpd	 -6 * SIZE(Y), %xmm5
	addpd	%xmm8,   %xmm5
	movaps	%xmm5,  -6 * SIZE(Y)

	pshufd	$0x4e,   %xmm6, %xmm8
	mulpd	ALPHA_R, %xmm6
	mulpd	ALPHA_I, %xmm8
	addpd	 -4 * SIZE(Y), %xmm6
	addpd	%xmm8,   %xmm6
	movaps	%xmm6,  -4 * SIZE(Y)

	pshufd	$0x4e,   %xmm7, %xmm8
	mulpd	ALPHA_R, %xmm7
	mulpd	ALPHA_I, %xmm8
	addpd	 -2 * SIZE(Y), %xmm7
	addpd	%xmm8,   %xmm7
	movaps	%xmm7,  -2 * SIZE(Y)

	subq	$-16 * SIZE, X
	subq	$-16 * SIZE, Y
	ALIGN_3

.L15:
	movq	M,  %rax
	andq	$4, %rax
	jle	.L16

	movaps	-16 * SIZE(X), %xmm0
	movaps	-14 * SIZE(X), %xmm1
	movaps	-12 * SIZE(X), %xmm2
	movaps	-10 * SIZE(X), %xmm3

	pshufd	$0x4e,   %xmm0, %xmm8
	mulpd	ALPHA_R, %xmm0
	mulpd	ALPHA_I, %xmm8
	addpd	-16 * SIZE(Y), %xmm0
	addpd	%xmm8,   %xmm0
	movaps	%xmm0, -16 * SIZE(Y)

	pshufd	$0x4e,   %xmm1, %xmm8
	mulpd	ALPHA_R, %xmm1
	mulpd	ALPHA_I, %xmm8
	addpd	-14 * SIZE(Y), %xmm1
	addpd	%xmm8,   %xmm1
	movaps	%xmm1, -14 * SIZE(Y)

	pshufd	$0x4e,   %xmm2, %xmm8
	mulpd	ALPHA_R, %xmm2
	mulpd	ALPHA_I, %xmm8
	addpd	-12 * SIZE(Y), %xmm2
	addpd	%xmm8,   %xmm2
	movaps	%xmm2, -12 * SIZE(Y)

	pshufd	$0x4e,   %xmm3, %xmm8
	mulpd	ALPHA_R, %xmm3
	mulpd	ALPHA_I, %xmm8
	addpd	-10 * SIZE(Y), %xmm3
	addpd	%xmm8,   %xmm3
	movaps	%xmm3, -10 * SIZE(Y)

	addq	$8 * SIZE, X
	addq	$8 * SIZE, Y
	ALIGN_3

.L16:
	movq	M,  %rax
	andq	$2, %rax
	jle	.L17

	movaps	-16 * SIZE(X), %xmm0
	movaps	-14 * SIZE(X), %xmm1

	pshufd	$0x4e,   %xmm0, %xmm8
	mulpd	ALPHA_R, %xmm0
	mulpd	ALPHA_I, %xmm8
	addpd	-16 * SIZE(Y), %xmm0
	addpd	%xmm8,   %xmm0
	movaps	%xmm0, -16 * SIZE(Y)

	pshufd	$0x4e,   %xmm1, %xmm8
	mulpd	ALPHA_R, %xmm1
	mulpd	ALPHA_I, %xmm8
	addpd	-14 * SIZE(Y), %xmm1
	addpd	%xmm8,   %xmm1
	movaps	%xmm1, -14 * SIZE(Y)

	addq	$4 * SIZE, X
	addq	$4 * SIZE, Y
	ALIGN_3

.L17:
	movq	M,  %rax
	andq	$1, %rax
	jle	.L999

	movaps	-16 * SIZE(X), %xmm0

	pshufd	$0x4e,   %xmm0, %xmm8
	mulpd	ALPHA_R, %xmm0
	mulpd	ALPHA_I, %xmm8
	addpd	-16 * SIZE(Y), %xmm0
	addpd	%xmm8,   %xmm0
	movaps	%xmm0, -16 * SIZE(Y)
	jmp	.L999
	ALIGN_3

.L20:
	movq	M,  %rax
	sarq	$3, %rax
	jle	.L25

	movsd	-16 * SIZE(X), %xmm0
	movhps	-15 * SIZE(X), %xmm0
	movsd	-14 * SIZE(X), %xmm1
	movhps	-13 * SIZE(X), %xmm1
	movsd	-12 * SIZE(X), %xmm2
	movhps	-11 * SIZE(X), %xmm2
	movsd	-10 * SIZE(X), %xmm3
	movhps	 -9 * SIZE(X), %xmm3

	decq	%rax
	jle	.L22
	ALIGN_3

.L21:
	movsd	 -8 * SIZE(X), %xmm4
	movhps	 -7 * SIZE(X), %xmm4

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	pshufd	$0x4e,   %xmm0, %xmm8
	mulpd	ALPHA_R, %xmm0
	mulpd	ALPHA_I, %xmm8
	addpd	-16 * SIZE(Y), %xmm0
	addpd	%xmm8,   %xmm0
	movaps	%xmm0, -16 * SIZE(Y)

	movsd	 -6 * SIZE(X), %xmm5
	movhps	 -5 * SIZE(X), %xmm5

	pshufd	$0x4e,   %xmm1, %xmm8
	mulpd	ALPHA_R, %xmm1
	mulpd	ALPHA_I, %xmm8
	addpd	-14 * SIZE(Y), %xmm1
	addpd	%xmm8,   %xmm1
	movaps	%xmm1, -14 * SIZE(Y)

	movsd	 -4 * SIZE(X), %xmm6
	movhps	 -3 * SIZE(X), %xmm6

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	pshufd	$0x4e,   %xmm2, %xmm8
	mulpd	ALPHA_R, %xmm2
	mulpd	ALPHA_I, %xmm8
	addpd	-12 * SIZE(Y), %xmm2
	addpd	%xmm8,   %xmm2
	movaps	%xmm2, -12 * SIZE(Y)

	movsd	 -2 * SIZE(X), %xmm7
	movhps	 -1 * SIZE(X), %xmm7

	pshufd	$0x4e,   %xmm3, %xmm8
	mulpd	ALPHA_R, %xmm3
	mulpd	ALPHA_I, %xmm8
	addpd	-10 * SIZE(Y), %xmm3
	addpd	%xmm8,   %xmm3
	movaps	%xmm3, -10 * SIZE(Y)

	movsd	  0 * SIZE(X), %xmm0
	movhps	  1 * SIZE(X), %xmm0

#if defined(PREFETCHW) && !defined(FETCH128)
	PREFETCHW (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	pshufd	$0x4e,   %xmm4, %xmm8
	mulpd	ALPHA_R, %xmm4
	mulpd	ALPHA_I, %xmm8
	addpd	 -8 * SIZE(Y), %xmm4
	addpd	%xmm8,   %xmm4
	movaps	%xmm4,  -8 * SIZE(Y)

	movsd	  2 * SIZE(X), %xmm1
	movhps	  3 * SIZE(X), %xmm1

	pshufd	$0x4e,   %xmm5, %xmm8
	mulpd	ALPHA_R, %xmm5
	mulpd	ALPHA_I, %xmm8
	addpd	 -6 * SIZE(Y), %xmm5
	addpd	%xmm8,   %xmm5
	movaps	%xmm5,  -6 * SIZE(Y)

	movsd	  4 * SIZE(X), %xmm2
	movhps	  5 * SIZE(X), %xmm2

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	pshufd	$0x4e,   %xmm6, %xmm8
	mulpd	ALPHA_R, %xmm6
	mulpd	ALPHA_I, %xmm8
	addpd	 -4 * SIZE(Y), %xmm6
	addpd	%xmm8,   %xmm6
	movaps	%xmm6,  -4 * SIZE(Y)

	movsd	  6 * SIZE(X), %xmm3
	movhps	  7 * SIZE(X), %xmm3

	pshufd	$0x4e,   %xmm7, %xmm8
	mulpd	ALPHA_R, %xmm7
	mulpd	ALPHA_I, %xmm8
	addpd	 -2 * SIZE(Y), %xmm7
	addpd	%xmm8,   %xmm7
	movaps	%xmm7,  -2 * SIZE(Y)

	subq	$-16 * SIZE, X
	subq	$-16 * SIZE, Y
	decq	%rax
	jg	.L21
	ALIGN_3

.L22:
	movsd	 -8 * SIZE(X), %xmm4
	movhps	 -7 * SIZE(X), %xmm4

	pshufd	$0x4e,   %xmm0, %xmm8
	mulpd	ALPHA_R, %xmm0
	mulpd	ALPHA_I, %xmm8
	addpd	-16 * SIZE(Y), %xmm0
	addpd	%xmm8,   %xmm0
	movaps	%xmm0, -16 * SIZE(Y)

	movsd	 -6 * SIZE(X), %xmm5
	movhps	 -5 * SIZE(X), %xmm5

	pshufd	$0x4e,   %xmm1, %xmm8
	mulpd	ALPHA_R, %xmm1
	mulpd	ALPHA_I, %xmm8
	addpd	-14 * SIZE(Y), %xmm1
	addpd	%xmm8,   %xmm1
	movaps	%xmm1, -14 * SIZE(Y)

	movsd	 -4 * SIZE(X), %xmm6
	movhps	 -3 * SIZE(X), %xmm6

	pshufd	$0x4e,   %xmm2, %xmm8
	mulpd	ALPHA_R, %xmm2
	mulpd	ALPHA_I, %xmm8
	addpd	-12 * SIZE(Y), %xmm2
	addpd	%xmm8,   %xmm2
	movaps	%xmm2, -12 * SIZE(Y)

	movsd	 -2 * SIZE(X), %xmm7
	movhps	 -1 * SIZE(X), %xmm7

	pshufd	$0x4e,   %xmm3, %xmm8
	mulpd	ALPHA_R, %xmm3
	mulpd	ALPHA_I, %xmm8
	addpd	-10 * SIZE(Y), %xmm3
	addpd	%xmm8,   %xmm3
	movaps	%xmm3, -10 * SIZE(Y)

	pshufd	$0x4e,   %xmm4, %xmm8
	mulpd	ALPHA_R, %xmm4
	mulpd	ALPHA_I, %xmm8
	addpd	 -8 * SIZE(Y), %xmm4
	addpd	%xmm8,   %xmm4
	movaps	%xmm4,  -8 * SIZE(Y)

	pshufd	$0x4e,   %xmm5, %xmm8
	mulpd	ALPHA_R, %xmm5
	mulpd	ALPHA_I, %xmm8
	addpd	 -6 * SIZE(Y), %xmm5
	addpd	%xmm8,   %xmm5
	movaps	%xmm5,  -6 * SIZE(Y)

	pshufd	$0x4e,   %xmm6, %xmm8
	mulpd	ALPHA_R, %xmm6
	mulpd	ALPHA_I, %xmm8
	addpd	 -4 * SIZE(Y), %xmm6
	addpd	%xmm8,   %xmm6
	movaps	%xmm6,  -4 * SIZE(Y)

	pshufd	$0x4e,   %xmm7, %xmm8
	mulpd	ALPHA_R, %xmm7
	mulpd	ALPHA_I, %xmm8
	addpd	 -2 * SIZE(Y), %xmm7
	addpd	%xmm8,   %xmm7
	movaps	%xmm7,  -2 * SIZE(Y)

	subq	$-16 * SIZE, X
	subq	$-16 * SIZE, Y
	ALIGN_3

.L25:
	movq	M,  %rax
	andq	$4, %rax
	jle	.L26

	movsd	-16 * SIZE(X), %xmm0
	movhps	-15 * SIZE(X), %xmm0
	movsd	-14 * SIZE(X), %xmm1
	movhps	-13 * SIZE(X), %xmm1

	pshufd	$0x4e,   %xmm0, %xmm8
	mulpd	ALPHA_R, %xmm0
	mulpd	ALPHA_I, %xmm8
	addpd	-16 * SIZE(Y), %xmm0
	addpd	%xmm8,   %xmm0
	movaps	%xmm0, -16 * SIZE(Y)

	pshufd	$0x4e,   %xmm1, %xmm8
	mulpd	ALPHA_R, %xmm1
	mulpd	ALPHA_I, %xmm8
	addpd	-14 * SIZE(Y), %xmm1
	addpd	%xmm8,   %xmm1
	movaps	%xmm1, -14 * SIZE(Y)

	movsd	-12 * SIZE(X), %xmm2
	movhps	-11 * SIZE(X), %xmm2
	movsd	-10 * SIZE(X), %xmm3
	movhps	 -9 * SIZE(X), %xmm3

	pshufd	$0x4e,   %xmm2, %xmm8
	mulpd	ALPHA_R, %xmm2
	mulpd	ALPHA_I, %xmm8
	addpd	-12 * SIZE(Y), %xmm2
	addpd	%xmm8,   %xmm2
	movaps	%xmm2, -12 * SIZE(Y)

	pshufd	$0x4e,   %xmm3, %xmm8
	mulpd	ALPHA_R, %xmm3
	mulpd	ALPHA_I, %xmm8
	addpd	-10 * SIZE(Y), %xmm3
	addpd	%xmm8,   %xmm3
	movaps	%xmm3, -10 * SIZE(Y)

	addq	$8 * SIZE, X
	addq	$8 * SIZE, Y
	ALIGN_3

.L26:
	movq	M,  %rax
	andq	$2, %rax
	jle	.L27

	movsd	-16 * SIZE(X), %xmm0
	movhps	-15 * SIZE(X), %xmm0

	pshufd	$0x4e,   %xmm0, %xmm8
	mulpd	ALPHA_R, %xmm0
	mulpd	ALPHA_I, %xmm8
	addpd	-16 * SIZE(Y), %xmm0
	addpd	%xmm8,   %xmm0
	movaps	%xmm0, -16 * SIZE(Y)

	movsd	-14 * SIZE(X), %xmm1
	movhps	-13 * SIZE(X), %xmm1

	pshufd	$0x4e,   %xmm1, %xmm8
	mulpd	ALPHA_R, %xmm1
	mulpd	ALPHA_I, %xmm8
	addpd	-14 * SIZE(Y), %xmm1
	addpd	%xmm8,   %xmm1
	movaps	%xmm1, -14 * SIZE(Y)

	addq	$4 * SIZE, X
	addq	$4 * SIZE, Y
	ALIGN_3

.L27:
	movq	M,  %rax
	andq	$1, %rax
	jle	.L999

	movsd	-16 * SIZE(X), %xmm0
	movhps	-15 * SIZE(X), %xmm0

	pshufd	$0x4e,   %xmm0, %xmm8
	mulpd	ALPHA_R, %xmm0
	mulpd	ALPHA_I, %xmm8
	addpd	-16 * SIZE(Y), %xmm0
	addpd	%xmm8,   %xmm0
	movaps	%xmm0, -16 * SIZE(Y)
	jmp	.L999
	ALIGN_3

.L30:
	testq	$SIZE, X
	jne	.L40

	movaps	-16 * SIZE(X), %xmm1

	pshufd	$0x4e,   %xmm1, %xmm8
	mulpd	ALPHA_R, %xmm1
	mulpd	ALPHA_I, %xmm8
	addpd	%xmm8,   %xmm1
	xorps	%xmm0,   %xmm0
	SHUFPD_1 %xmm1,  %xmm0

	xorps	%xmm4,   %xmm4
	movhps	-16 * SIZE(Y), %xmm4

	addpd	%xmm0,   %xmm4
	movhps	%xmm4, -16 * SIZE(Y)
	movaps	%xmm1,   %xmm0

	addq	$2 * SIZE, X
	addq	$1 * SIZE, Y
	decq	M
	jle	.L39

	movq	M,  %rax
	sarq	$3, %rax
	jle	.L35

	movaps	-16 * SIZE(X), %xmm1
	movaps	-14 * SIZE(X), %xmm2
	movaps	-12 * SIZE(X), %xmm3

	decq	%rax
	jle	.L32
	ALIGN_3

.L31:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	pshufd	$0x4e,   %xmm1, %xmm8
	mulpd	ALPHA_R, %xmm1
	mulpd	ALPHA_I, %xmm8
	addpd	%xmm8,   %xmm1
	SHUFPD_1 %xmm1,  %xmm0

	addpd	-16 * SIZE(Y), %xmm0
	movaps	%xmm0, -16 * SIZE(Y)
	movaps	-10 * SIZE(X), %xmm0

	pshufd	$0x4e,   %xmm2, %xmm8
	mulpd	ALPHA_R, %xmm2
	mulpd	ALPHA_I, %xmm8
	addpd	%xmm8,   %xmm2
	SHUFPD_1 %xmm2,  %xmm1

	addpd	-14 * SIZE(Y), %xmm1
	movaps	%xmm1, -14 * SIZE(Y)
	movaps	 -8 * SIZE(X), %xmm1

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	pshufd	$0x4e,   %xmm3, %xmm8
	mulpd	ALPHA_R, %xmm3
	mulpd	ALPHA_I, %xmm8
	addpd	%xmm8,   %xmm3
	SHUFPD_1 %xmm3,  %xmm2

	addpd	-12 * SIZE(Y), %xmm2
	movaps	%xmm2, -12 * SIZE(Y)
	movaps	 -6 * SIZE(X), %xmm2

	pshufd	$0x4e,   %xmm0, %xmm8
	mulpd	ALPHA_R, %xmm0
	mulpd	ALPHA_I, %xmm8
	addpd	%xmm8,   %xmm0
	SHUFPD_1 %xmm0,  %xmm3

	addpd	-10 * SIZE(Y), %xmm3
	movaps	%xmm3, -10 * SIZE(Y)
	movaps	 -4 * SIZE(X), %xmm3

#if defined(PREFETCHW) && !defined(FETCH128)
	PREFETCHW (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	pshufd	$0x4e,   %xmm1, %xmm8
	mulpd	ALPHA_R, %xmm1
	mulpd	ALPHA_I, %xmm8
	addpd	%xmm8,   %xmm1
	SHUFPD_1 %xmm1,  %xmm0

	addpd	 -8 * SIZE(Y), %xmm0
	movaps	%xmm0,  -8 * SIZE(Y)
	movaps	 -2 * SIZE(X), %xmm0

	pshufd	$0x4e,   %xmm2, %xmm8
	mulpd	ALPHA_R, %xmm2
	mulpd	ALPHA_I, %xmm8
	addpd	%xmm8,   %xmm2
	SHUFPD_1 %xmm2,  %xmm1

	addpd	 -6 * SIZE(Y), %xmm1
	movaps	%xmm1,  -6 * SIZE(Y)
	movaps	  0 * SIZE(X), %xmm1

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	pshufd	$0x4e,   %xmm3, %xmm8
	mulpd	ALPHA_R, %xmm3
	mulpd	ALPHA_I, %xmm8
	addpd	%xmm8,   %xmm3
	SHUFPD_1 %xmm3,  %xmm2

	addpd	 -4 * SIZE(Y), %xmm2
	movaps	%xmm2,  -4 * SIZE(Y)
	movaps	  2 * SIZE(X), %xmm2

	pshufd	$0x4e,   %xmm0, %xmm8
	mulpd	ALPHA_R, %xmm0
	mulpd	ALPHA_I, %xmm8
	addpd	%xmm8,   %xmm0
	SHUFPD_1 %xmm0,  %xmm3

	addpd	 -2 * SIZE(Y), %xmm3
	movaps	%xmm3,  -2 * SIZE(Y)
	movaps	  4 * SIZE(X), %xmm3

	subq	$-16 * SIZE, X
	subq	$-16 * SIZE, Y
	decq	%rax
	jg	.L31
	ALIGN_3

.L32:
	pshufd	$0x4e,   %xmm1, %xmm8
	mulpd	ALPHA_R, %xmm1
	mulpd	ALPHA_I, %xmm8
	addpd	%xmm8,   %xmm1
	SHUFPD_1 %xmm1,  %xmm0

	addpd	-16 * SIZE(Y), %xmm0
	movaps	%xmm0, -16 * SIZE(Y)
	movaps	-10 * SIZE(X), %xmm0

	pshufd	$0x4e,   %xmm2, %xmm8
	mulpd	ALPHA_R, %xmm2
	mulpd	ALPHA_I, %xmm8
	addpd	%xmm8,   %xmm2
	SHUFPD_1 %xmm2,  %xmm1

	addpd	-14 * SIZE(Y), %xmm1
	movaps	%xmm1, -14 * SIZE(Y)
	movaps	 -8 * SIZE(X), %xmm1

	pshufd	$0x4e,   %xmm3, %xmm8
	mulpd	ALPHA_R, %xmm3
	mulpd	ALPHA_I, %xmm8
	addpd	%xmm8,   %xmm3
	SHUFPD_1 %xmm3,  %xmm2

	addpd	-12 * SIZE(Y), %xmm2
	movaps	%xmm2, -12 * SIZE(Y)
	movaps	 -6 * SIZE(X), %xmm2

	pshufd	$0x4e,   %xmm0, %xmm8
	mulpd	ALPHA_R, %xmm0
	mulpd	ALPHA_I, %xmm8
	addpd	%xmm8,   %xmm0
	SHUFPD_1 %xmm0,  %xmm3

	addpd	-10 * SIZE(Y), %xmm3
	movaps	%xmm3, -10 * SIZE(Y)
	movaps	 -4 * SIZE(X), %xmm3

	pshufd	$0x4e,   %xmm1, %xmm8
	mulpd	ALPHA_R, %xmm1
	mulpd	ALPHA_I, %xmm8
	addpd	%xmm8,   %xmm1
	SHUFPD_1 %xmm1,  %xmm0

	addpd	 -8 * SIZE(Y), %xmm0
	movaps	%xmm0,  -8 * SIZE(Y)
	movaps	 -2 * SIZE(X), %xmm0

	pshufd	$0x4e,   %xmm2, %xmm8
	mulpd	ALPHA_R, %xmm2
	mulpd	ALPHA_I, %xmm8
	addpd	%xmm8,   %xmm2
	SHUFPD_1 %xmm2,  %xmm1

	addpd	 -6 * SIZE(Y), %xmm1
	movaps	%xmm1,  -6 * SIZE(Y)

	pshufd	$0x4e,   %xmm3, %xmm8
	mulpd	ALPHA_R, %xmm3
	mulpd	ALPHA_I, %xmm8
	addpd	%xmm8,   %xmm3
	SHUFPD_1 %xmm3,  %xmm2

	addpd	 -4 * SIZE(Y), %xmm2
	movaps	%xmm2,  -4 * SIZE(Y)

	pshufd	$0x4e,   %xmm0, %xmm8
	mulpd	ALPHA_R, %xmm0
	mulpd	ALPHA_I, %xmm8
	addpd	%xmm8,   %xmm0
	SHUFPD_1 %xmm0,  %xmm3

	addpd	 -2 * SIZE(Y), %xmm3
	movaps	%xmm3,  -2 * SIZE(Y)

	subq	$-16 * SIZE, X
	subq	$-16 * SIZE, Y
	ALIGN_3

.L35:
	movq	M,  %rax
	andq	$4, %rax
	jle	.L36

	movaps	-16 * SIZE(X), %xmm1
	movaps	-14 * SIZE(X), %xmm2
	movaps	-12 * SIZE(X), %xmm3
	movaps	-10 * SIZE(X), %xmm4

	pshufd	$0x4e,   %xmm1, %xmm8
	mulpd	ALPHA_R, %xmm1
	mulpd	ALPHA_I, %xmm8
	addpd	%xmm8,   %xmm1
	SHUFPD_1 %xmm1,  %xmm0

	addpd	-16 * SIZE(Y), %xmm0
	movaps	%xmm0, -16 * SIZE(Y)

	pshufd	$0x4e,   %xmm2, %xmm8
	mulpd	ALPHA_R, %xmm2
	mulpd	ALPHA_I, %xmm8
	addpd	%xmm8,   %xmm2
	SHUFPD_1 %xmm2,  %xmm1

	addpd	-14 * SIZE(Y), %xmm1
	movaps	%xmm1, -14 * SIZE(Y)

	pshufd	$0x4e,   %xmm3, %xmm8
	mulpd	ALPHA_R, %xmm3
	mulpd	ALPHA_I, %xmm8
	addpd	%xmm8,   %xmm3
	SHUFPD_1 %xmm3,  %xmm2

	addpd	-12 * SIZE(Y), %xmm2
	movaps	%xmm2, -12 * SIZE(Y)

	pshufd	$0x4e,   %xmm4, %xmm8
	mulpd	ALPHA_R, %xmm4
	mulpd	ALPHA_I, %xmm8
	addpd	%xmm8,   %xmm4
	SHUFPD_1 %xmm4,  %xmm3

	addpd	-10 * SIZE(Y), %xmm3
	movaps	%xmm3, -10 * SIZE(Y)
	movaps	%xmm4,   %xmm0

	addq	$8 * SIZE, X
	addq	$8 * SIZE, Y
	ALIGN_3

.L36:
	movq	M,  %rax
	andq	$2, %rax
	jle	.L37

	movaps	-16 * SIZE(X), %xmm1
	movaps	-14 * SIZE(X), %xmm2

	pshufd	$0x4e,   %xmm1, %xmm8
	mulpd	ALPHA_R, %xmm1
	mulpd	ALPHA_I, %xmm8
	addpd	%xmm8,   %xmm1
	SHUFPD_1 %xmm1,  %xmm0

	addpd	-16 * SIZE(Y), %xmm0
	movaps	%xmm0, -16 * SIZE(Y)

	pshufd	$0x4e,   %xmm2, %xmm8
	mulpd	ALPHA_R, %xmm2
	mulpd	ALPHA_I, %xmm8
	addpd	%xmm8,   %xmm2
	SHUFPD_1 %xmm2,  %xmm1

	addpd	-14 * SIZE(Y), %xmm1
	movaps	%xmm1, -14 * SIZE(Y)
	movaps	%xmm2,   %xmm0

	addq	$4 * SIZE, X
	addq	$4 * SIZE, Y
	ALIGN_3

.L37:
	movq	M,  %rax
	andq	$1, %rax
	jle	.L39

	movaps	-16 * SIZE(X), %xmm1

	pshufd	$0x4e,   %xmm1, %xmm8
	mulpd	ALPHA_R, %xmm1
	mulpd	ALPHA_I, %xmm8
	addpd	%xmm8,   %xmm1
	SHUFPD_1 %xmm1,  %xmm0

	addpd	-16 * SIZE(Y), %xmm0
	movaps	%xmm0, -16 * SIZE(Y)
	movaps	%xmm1,   %xmm0

	addq	$2 * SIZE, X
	addq	$2 * SIZE, Y
	ALIGN_3

.L39:
	SHUFPD_1 %xmm0,  %xmm0

	addsd	-16 * SIZE(Y), %xmm0
	movlps	%xmm0, -16 * SIZE(Y)
	jmp	.L999
	ALIGN_3

.L40:
	movsd	-16 * SIZE(X), %xmm1
	movhps	-15 * SIZE(X), %xmm1

	pshufd	$0x4e,   %xmm1, %xmm8
	mulpd	ALPHA_R, %xmm1
	mulpd	ALPHA_I, %xmm8
	addpd	%xmm8,   %xmm1
	xorps	%xmm0,   %xmm0
	SHUFPD_1 %xmm1,  %xmm0

	xorps	%xmm4,   %xmm4
	movhps	-16 * SIZE(Y), %xmm4

	addpd	%xmm0,   %xmm4
	movhps	%xmm4, -16 * SIZE(Y)
	movaps	%xmm1,   %xmm0

	addq	$2 * SIZE, X
	addq	$1 * SIZE, Y
	decq	M
	jle	.L49

	movq	M,  %rax
	sarq	$3, %rax
	jle	.L45

	movsd	-16 * SIZE(X), %xmm1
	movhps	-15 * SIZE(X), %xmm1
	movsd	-14 * SIZE(X), %xmm2
	movhps	-13 * SIZE(X), %xmm2
	movsd	-12 * SIZE(X), %xmm3
	movhps	-11 * SIZE(X), %xmm3

	decq	%rax
	jle	.L42
	ALIGN_3

.L41:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	pshufd	$0x4e,   %xmm1, %xmm8
	mulpd	ALPHA_R, %xmm1
	mulpd	ALPHA_I, %xmm8
	addpd	%xmm8,   %xmm1
	SHUFPD_1 %xmm1,  %xmm0

	addpd	-16 * SIZE(Y), %xmm0
	movaps	%xmm0, -16 * SIZE(Y)
	movsd	-10 * SIZE(X), %xmm0
	movhps	 -9 * SIZE(X), %xmm0

	pshufd	$0x4e,   %xmm2, %xmm8
	mulpd	ALPHA_R, %xmm2
	mulpd	ALPHA_I, %xmm8
	addpd	%xmm8,   %xmm2
	SHUFPD_1 %xmm2,  %xmm1

	addpd	-14 * SIZE(Y), %xmm1
	movaps	%xmm1, -14 * SIZE(Y)
	movsd	 -8 * SIZE(X), %xmm1
	movhps	 -7 * SIZE(X), %xmm1

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	pshufd	$0x4e,   %xmm3, %xmm8
	mulpd	ALPHA_R, %xmm3
	mulpd	ALPHA_I, %xmm8
	addpd	%xmm8,   %xmm3
	SHUFPD_1 %xmm3,  %xmm2

	addpd	-12 * SIZE(Y), %xmm2
	movaps	%xmm2, -12 * SIZE(Y)
	movsd	 -6 * SIZE(X), %xmm2
	movhps	 -5 * SIZE(X), %xmm2

	pshufd	$0x4e,   %xmm0, %xmm8
	mulpd	ALPHA_R, %xmm0
	mulpd	ALPHA_I, %xmm8
	addpd	%xmm8,   %xmm0
	SHUFPD_1 %xmm0,  %xmm3

	addpd	-10 * SIZE(Y), %xmm3
	movaps	%xmm3, -10 * SIZE(Y)
	movsd	 -4 * SIZE(X), %xmm3
	movhps	 -3 * SIZE(X), %xmm3

#if defined(PREFETCHW) && !defined(FETCH128)
	PREFETCHW (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	pshufd	$0x4e,   %xmm1, %xmm8
	mulpd	ALPHA_R, %xmm1
	mulpd	ALPHA_I, %xmm8
	addpd	%xmm8,   %xmm1
	SHUFPD_1 %xmm1,  %xmm0

	addpd	 -8 * SIZE(Y), %xmm0
	movaps	%xmm0,  -8 * SIZE(Y)
	movsd	 -2 * SIZE(X), %xmm0
	movhps	 -1 * SIZE(X), %xmm0

	pshufd	$0x4e,   %xmm2, %xmm8
	mulpd	ALPHA_R, %xmm2
	mulpd	ALPHA_I, %xmm8
	addpd	%xmm8,   %xmm2
	SHUFPD_1 %xmm2,  %xmm1

	addpd	 -6 * SIZE(Y), %xmm1
	movaps	%xmm1,  -6 * SIZE(Y)
	movsd	  0 * SIZE(X), %xmm1
	movhps	  1 * SIZE(X), %xmm1

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	pshufd	$0x4e,   %xmm3, %xmm8
	mulpd	ALPHA_R, %xmm3
	mulpd	ALPHA_I, %xmm8
	addpd	%xmm8,   %xmm3
	SHUFPD_1 %xmm3,  %xmm2

	addpd	 -4 * SIZE(Y), %xmm2
	movaps	%xmm2,  -4 * SIZE(Y)
	movsd	  2 * SIZE(X), %xmm2
	movhps	  3 * SIZE(X), %xmm2

	pshufd	$0x4e,   %xmm0, %xmm8
	mulpd	ALPHA_R, %xmm0
	mulpd	ALPHA_I, %xmm8
	addpd	%xmm8,   %xmm0
	SHUFPD_1 %xmm0,  %xmm3

	addpd	 -2 * SIZE(Y), %xmm3
	movaps	%xmm3,  -2 * SIZE(Y)
	movsd	  4 * SIZE(X), %xmm3
	movhps	  5 * SIZE(X), %xmm3

	subq	$-16 * SIZE, X
	subq	$-16 * SIZE, Y
	decq	%rax
	jg	.L41
	ALIGN_3

.L42:
	pshufd	$0x4e,   %xmm1, %xmm8
	mulpd	ALPHA_R, %xmm1
	mulpd	ALPHA_I, %xmm8
	addpd	%xmm8,   %xmm1
	SHUFPD_1 %xmm1,  %xmm0

	addpd	-16 * SIZE(Y), %xmm0
	movaps	%xmm0, -16 * SIZE(Y)
	movsd	-10 * SIZE(X), %xmm0
	movhps	 -9 * SIZE(X), %xmm0

	pshufd	$0x4e,   %xmm2, %xmm8
	mulpd	ALPHA_R, %xmm2
	mulpd	ALPHA_I, %xmm8
	addpd	%xmm8,   %xmm2
	SHUFPD_1 %xmm2,  %xmm1

	addpd	-14 * SIZE(Y), %xmm1
	movaps	%xmm1, -14 * SIZE(Y)
	movsd	 -8 * SIZE(X), %xmm1
	movhps	 -7 * SIZE(X), %xmm1

	pshufd	$0x4e,   %xmm3, %xmm8
	mulpd	ALPHA_R, %xmm3
	mulpd	ALPHA_I, %xmm8
	addpd	%xmm8,   %xmm3
	SHUFPD_1 %xmm3,  %xmm2

	addpd	-12 * SIZE(Y), %xmm2
	movaps	%xmm2, -12 * SIZE(Y)
	movsd	 -6 * SIZE(X), %xmm2
	movhps	 -5 * SIZE(X), %xmm2

	pshufd	$0x4e,   %xmm0, %xmm8
	mulpd	ALPHA_R, %xmm0
	mulpd	ALPHA_I, %xmm8
	addpd	%xmm8,   %xmm0
	SHUFPD_1 %xmm0,  %xmm3

	addpd	-10 * SIZE(Y), %xmm3
	movaps	%xmm3, -10 * SIZE(Y)
	movsd	 -4 * SIZE(X), %xmm3
	movhps	 -3 * SIZE(X), %xmm3

	pshufd	$0x4e,   %xmm1, %xmm8
	mulpd	ALPHA_R, %xmm1
	mulpd	ALPHA_I, %xmm8
	addpd	%xmm8,   %xmm1
	SHUFPD_1 %xmm1,  %xmm0

	addpd	 -8 * SIZE(Y), %xmm0
	movaps	%xmm0,  -8 * SIZE(Y)
	movsd	 -2 * SIZE(X), %xmm0
	movhps	 -1 * SIZE(X), %xmm0

	pshufd	$0x4e,   %xmm2, %xmm8
	mulpd	ALPHA_R, %xmm2
	mulpd	ALPHA_I, %xmm8
	addpd	%xmm8,   %xmm2
	SHUFPD_1 %xmm2,  %xmm1

	addpd	 -6 * SIZE(Y), %xmm1
	movaps	%xmm1,  -6 * SIZE(Y)

	pshufd	$0x4e,   %xmm3, %xmm8
	mulpd	ALPHA_R, %xmm3
	mulpd	ALPHA_I, %xmm8
	addpd	%xmm8,   %xmm3
	SHUFPD_1 %xmm3,  %xmm2

	addpd	 -4 * SIZE(Y), %xmm2
	movaps	%xmm2,  -4 * SIZE(Y)

	pshufd	$0x4e,   %xmm0, %xmm8
	mulpd	ALPHA_R, %xmm0
	mulpd	ALPHA_I, %xmm8
	addpd	%xmm8,   %xmm0
	SHUFPD_1 %xmm0,  %xmm3

	addpd	 -2 * SIZE(Y), %xmm3
	movaps	%xmm3,  -2 * SIZE(Y)

	subq	$-16 * SIZE, X
	subq	$-16 * SIZE, Y
	ALIGN_3

.L45:
	movq	M,  %rax
	andq	$4, %rax
	jle	.L46

	movsd	-16 * SIZE(X), %xmm1
	movhps	-15 * SIZE(X), %xmm1
	movsd	-14 * SIZE(X), %xmm2
	movhps	-13 * SIZE(X), %xmm2
	movsd	-12 * SIZE(X), %xmm3
	movhps	-11 * SIZE(X), %xmm3
	movsd	-10 * SIZE(X), %xmm4
	movhps	 -9 * SIZE(X), %xmm4

	pshufd	$0x4e,   %xmm1, %xmm8
	mulpd	ALPHA_R, %xmm1
	mulpd	ALPHA_I, %xmm8
	addpd	%xmm8,   %xmm1
	SHUFPD_1 %xmm1,  %xmm0

	addpd	-16 * SIZE(Y), %xmm0
	movaps	%xmm0, -16 * SIZE(Y)

	pshufd	$0x4e,   %xmm2, %xmm8
	mulpd	ALPHA_R, %xmm2
	mulpd	ALPHA_I, %xmm8
	addpd	%xmm8,   %xmm2
	SHUFPD_1 %xmm2,  %xmm1

	addpd	-14 * SIZE(Y), %xmm1
	movaps	%xmm1, -14 * SIZE(Y)

	pshufd	$0x4e,   %xmm3, %xmm8
	mulpd	ALPHA_R, %xmm3
	mulpd	ALPHA_I, %xmm8
	addpd	%xmm8,   %xmm3
	SHUFPD_1 %xmm3,  %xmm2

	addpd	-12 * SIZE(Y), %xmm2
	movaps	%xmm2, -12 * SIZE(Y)

	pshufd	$0x4e,   %xmm4, %xmm8
	mulpd	ALPHA_R, %xmm4
	mulpd	ALPHA_I, %xmm8
	addpd	%xmm8,   %xmm4
	SHUFPD_1 %xmm4,  %xmm3

	addpd	-10 * SIZE(Y), %xmm3
	movaps	%xmm3, -10 * SIZE(Y)
	movaps	%xmm4,   %xmm0

	addq	$8 * SIZE, X
	addq	$8 * SIZE, Y
	ALIGN_3

.L46:
	movq	M,  %rax
	andq	$2, %rax
	jle	.L47

	movsd	-16 * SIZE(X), %xmm1
	movhps	-15 * SIZE(X), %xmm1
	movsd	-14 * SIZE(X), %xmm2
	movhps	-13 * SIZE(X), %xmm2

	pshufd	$0x4e,   %xmm1, %xmm8
	mulpd	ALPHA_R, %xmm1
	mulpd	ALPHA_I, %xmm8
	addpd	%xmm8,   %xmm1
	SHUFPD_1 %xmm1,  %xmm0

	addpd	-16 * SIZE(Y), %xmm0
	movaps	%xmm0, -16 * SIZE(Y)

	pshufd	$0x4e,   %xmm2, %xmm8
	mulpd	ALPHA_R, %xmm2
	mulpd	ALPHA_I, %xmm8
	addpd	%xmm8,   %xmm2
	SHUFPD_1 %xmm2,  %xmm1

	addpd	-14 * SIZE(Y), %xmm1
	movaps	%xmm1, -14 * SIZE(Y)
	movaps	%xmm2,   %xmm0

	addq	$4 * SIZE, X
	addq	$4 * SIZE, Y
	ALIGN_3

.L47:
	movq	M,  %rax
	andq	$1, %rax
	jle	.L49

	movsd	-16 * SIZE(X), %xmm1
	movhps	-15 * SIZE(X), %xmm1

	pshufd	$0x4e,   %xmm1, %xmm8
	mulpd	ALPHA_R, %xmm1
	mulpd	ALPHA_I, %xmm8
	addpd	%xmm8,   %xmm1
	SHUFPD_1 %xmm1,  %xmm0

	addpd	-16 * SIZE(Y), %xmm0
	movaps	%xmm0, -16 * SIZE(Y)
	movaps	%xmm1,   %xmm0

	addq	$2 * SIZE, X
	addq	$2 * SIZE, Y
	ALIGN_3

.L49:
	SHUFPD_1 %xmm0,  %xmm0

	addsd	-16 * SIZE(Y), %xmm0
	movlps	%xmm0, -16 * SIZE(Y)
	jmp	.L999
	ALIGN_3

.L50:
#ifndef CONJ
	movaps	%xmm0, %xmm14		#  a   0

	pxor	%xmm15, %xmm15		#  0   0
	subsd	%xmm1,  %xmm15		# -b   0

	unpcklpd %xmm14, %xmm15		# -b   a
	unpcklpd %xmm1,  %xmm14		#  a   b
#else
	movaps	%xmm0, %xmm14		#  a   0
	movaps	%xmm1, %xmm15		#  b   0

	pxor	%xmm13, %xmm13		#  0   0
	subsd	%xmm0,  %xmm13		# -a   0

	unpcklpd %xmm13, %xmm15		#  b   -a
	unpcklpd %xmm1,  %xmm14		#  a   b
#endif

	movq	Y, YY
	movq	M,  %rax
//If incx==0 || incy==0, avoid unloop and jump to end.
	cmpq	$0, INCX
	je  .L58
	cmpq	$0, INCY
	je	.L58

	sarq	$3, %rax
	jle	.L55

	MOVDDUP( 0 * SIZE, X, %xmm0)
	MOVDDUP( 1 * SIZE, X, %xmm1)
	addq	 INCX, X
	MOVDDUP( 0 * SIZE, X, %xmm2)
	MOVDDUP( 1 * SIZE, X, %xmm3)
	addq	 INCX, X
	MOVDDUP( 0 * SIZE, X, %xmm4)
	MOVDDUP( 1 * SIZE, X, %xmm5)
	addq	 INCX, X
	MOVDDUP( 0 * SIZE, X, %xmm6)
	MOVDDUP( 1 * SIZE, X, %xmm7)
	addq	 INCX, X

	movsd	 0 * SIZE(Y), %xmm8
	movhpd	 1 * SIZE(Y), %xmm8
	addq	 INCY, Y
	movsd	 0 * SIZE(Y), %xmm9
	movhpd	 1 * SIZE(Y), %xmm9
	addq	 INCY, Y
	movsd	 0 * SIZE(Y), %xmm10
	movhpd	 1 * SIZE(Y), %xmm10
	addq	 INCY, Y
	movsd	 0 * SIZE(Y), %xmm11
	movhpd	 1 * SIZE(Y), %xmm11
	addq	 INCY, Y

	mulpd	%xmm14, %xmm0
	mulpd	%xmm14, %xmm2
	mulpd	%xmm14, %xmm4
	mulpd	%xmm14, %xmm6

	decq	%rax
	jle	.L52
	ALIGN_3

.L51:
	addpd	%xmm0, %xmm8
	mulpd	%xmm15, %xmm1
	addpd	%xmm2, %xmm9
	mulpd	%xmm15, %xmm3
	addpd	%xmm4, %xmm10
	mulpd	%xmm15, %xmm5
	addpd	%xmm6, %xmm11
	mulpd	%xmm15, %xmm7

	addpd	%xmm1, %xmm8
	addpd	%xmm3, %xmm9
	addpd	%xmm5, %xmm10
	addpd	%xmm7, %xmm11

	MOVDDUP( 0 * SIZE, X, %xmm0)
	MOVDDUP( 1 * SIZE, X, %xmm1)
	addq	 INCX, X
	MOVDDUP( 0 * SIZE, X, %xmm2)
	MOVDDUP( 1 * SIZE, X, %xmm3)
	addq	 INCX, X
	MOVDDUP( 0 * SIZE, X, %xmm4)
	MOVDDUP( 1 * SIZE, X, %xmm5)
	addq	 INCX, X
	MOVDDUP( 0 * SIZE, X, %xmm6)
	MOVDDUP( 1 * SIZE, X, %xmm7)
	addq	 INCX, X

	mulpd	%xmm14, %xmm0
	mulpd	%xmm14, %xmm2
	mulpd	%xmm14, %xmm4
	mulpd	%xmm14, %xmm6

	movlpd	%xmm8,   0 * SIZE(YY)
	movhpd	%xmm8,   1 * SIZE(YY)
	addq	 INCY, YY
	movlpd	%xmm9,   0 * SIZE(YY)
	movhpd	%xmm9,   1 * SIZE(YY)
	addq	 INCY, YY
	movlpd	%xmm10,  0 * SIZE(YY)
	movhpd	%xmm10,  1 * SIZE(YY)
	addq	 INCY, YY
	movlpd	%xmm11,  0 * SIZE(YY)
	movhpd	%xmm11,  1 * SIZE(YY)
	addq	 INCY, YY

	movsd	 0 * SIZE(Y), %xmm8
	movhpd	 1 * SIZE(Y), %xmm8
	addq	 INCY, Y
	movsd	 0 * SIZE(Y), %xmm9
	movhpd	 1 * SIZE(Y), %xmm9
	addq	 INCY, Y
	movsd	 0 * SIZE(Y), %xmm10
	movhpd	 1 * SIZE(Y), %xmm10
	addq	 INCY, Y
	movsd	 0 * SIZE(Y), %xmm11
	movhpd	 1 * SIZE(Y), %xmm11
	addq	 INCY, Y

	addpd	%xmm0, %xmm8
	mulpd	%xmm15, %xmm1
	addpd	%xmm2, %xmm9
	mulpd	%xmm15, %xmm3
	addpd	%xmm4, %xmm10
	mulpd	%xmm15, %xmm5
	addpd	%xmm6, %xmm11
	mulpd	%xmm15, %xmm7

	addpd	%xmm1, %xmm8
	addpd	%xmm3, %xmm9
	addpd	%xmm5, %xmm10
	addpd	%xmm7, %xmm11

	MOVDDUP( 0 * SIZE, X, %xmm0)
	MOVDDUP( 1 * SIZE, X, %xmm1)
	addq	 INCX, X
	MOVDDUP( 0 * SIZE, X, %xmm2)
	MOVDDUP( 1 * SIZE, X, %xmm3)
	addq	 INCX, X
	MOVDDUP( 0 * SIZE, X, %xmm4)
	MOVDDUP( 1 * SIZE, X, %xmm5)
	addq	 INCX, X
	MOVDDUP( 0 * SIZE, X, %xmm6)
	MOVDDUP( 1 * SIZE, X, %xmm7)
	addq	 INCX, X

	mulpd	%xmm14, %xmm0
	mulpd	%xmm14, %xmm2
	mulpd	%xmm14, %xmm4
	mulpd	%xmm14, %xmm6

	movlpd	%xmm8,   0 * SIZE(YY)
	movhpd	%xmm8,   1 * SIZE(YY)
	addq	 INCY, YY
	movlpd	%xmm9,   0 * SIZE(YY)
	movhpd	%xmm9,   1 * SIZE(YY)
	addq	 INCY, YY
	movlpd	%xmm10,  0 * SIZE(YY)
	movhpd	%xmm10,  1 * SIZE(YY)
	addq	 INCY, YY
	movlpd	%xmm11,  0 * SIZE(YY)
	movhpd	%xmm11,  1 * SIZE(YY)
	addq	 INCY, YY

	movsd	 0 * SIZE(Y), %xmm8
	movhpd	 1 * SIZE(Y), %xmm8
	addq	 INCY, Y
	movsd	 0 * SIZE(Y), %xmm9
	movhpd	 1 * SIZE(Y), %xmm9
	addq	 INCY, Y
	movsd	 0 * SIZE(Y), %xmm10
	movhpd	 1 * SIZE(Y), %xmm10
	addq	 INCY, Y
	movsd	 0 * SIZE(Y), %xmm11
	movhpd	 1 * SIZE(Y), %xmm11
	addq	 INCY, Y

	decq	%rax
	jg	.L51
	ALIGN_3

.L52:
	addpd	%xmm0, %xmm8
	mulpd	%xmm15, %xmm1
	addpd	%xmm2, %xmm9
	mulpd	%xmm15, %xmm3
	addpd	%xmm4, %xmm10
	mulpd	%xmm15, %xmm5
	addpd	%xmm6, %xmm11
	mulpd	%xmm15, %xmm7

	addpd	%xmm1, %xmm8
	addpd	%xmm3, %xmm9
	addpd	%xmm5, %xmm10
	addpd	%xmm7, %xmm11

	MOVDDUP( 0 * SIZE, X, %xmm0)
	MOVDDUP( 1 * SIZE, X, %xmm1)
	addq	 INCX, X
	MOVDDUP( 0 * SIZE, X, %xmm2)
	MOVDDUP( 1 * SIZE, X, %xmm3)
	addq	 INCX, X
	MOVDDUP( 0 * SIZE, X, %xmm4)
	MOVDDUP( 1 * SIZE, X, %xmm5)
	addq	 INCX, X
	MOVDDUP( 0 * SIZE, X, %xmm6)
	MOVDDUP( 1 * SIZE, X, %xmm7)
	addq	 INCX, X

	mulpd	%xmm14, %xmm0
	mulpd	%xmm14, %xmm2
	mulpd	%xmm14, %xmm4
	mulpd	%xmm14, %xmm6

	movlpd	%xmm8,   0 * SIZE(YY)
	movhpd	%xmm8,   1 * SIZE(YY)
	addq	 INCY, YY
	movlpd	%xmm9,   0 * SIZE(YY)
	movhpd	%xmm9,   1 * SIZE(YY)
	addq	 INCY, YY
	movlpd	%xmm10,  0 * SIZE(YY)
	movhpd	%xmm10,  1 * SIZE(YY)
	addq	 INCY, YY
	movlpd	%xmm11,  0 * SIZE(YY)
	movhpd	%xmm11,  1 * SIZE(YY)
	addq	 INCY, YY

	movsd	 0 * SIZE(Y), %xmm8
	movhpd	 1 * SIZE(Y), %xmm8
	addq	 INCY, Y
	movsd	 0 * SIZE(Y), %xmm9
	movhpd	 1 * SIZE(Y), %xmm9
	addq	 INCY, Y
	movsd	 0 * SIZE(Y), %xmm10
	movhpd	 1 * SIZE(Y), %xmm10
	addq	 INCY, Y
	movsd	 0 * SIZE(Y), %xmm11
	movhpd	 1 * SIZE(Y), %xmm11
	addq	 INCY, Y

	addpd	%xmm0, %xmm8
	mulpd	%xmm15, %xmm1
	addpd	%xmm2, %xmm9
	mulpd	%xmm15, %xmm3
	addpd	%xmm4, %xmm10
	mulpd	%xmm15, %xmm5
	addpd	%xmm6, %xmm11
	mulpd	%xmm15, %xmm7

	addpd	%xmm1, %xmm8
	addpd	%xmm3, %xmm9
	addpd	%xmm5, %xmm10
	addpd	%xmm7, %xmm11

	movlpd	%xmm8,   0 * SIZE(YY)
	movhpd	%xmm8,   1 * SIZE(YY)
	addq	 INCY, YY
	movlpd	%xmm9,   0 * SIZE(YY)
	movhpd	%xmm9,   1 * SIZE(YY)
	addq	 INCY, YY
	movlpd	%xmm10,  0 * SIZE(YY)
	movhpd	%xmm10,  1 * SIZE(YY)
	addq	 INCY, YY
	movlpd	%xmm11,  0 * SIZE(YY)
	movhpd	%xmm11,  1 * SIZE(YY)
	addq	 INCY, YY
	ALIGN_3

.L55:
	movq	M,  %rax
	andq	$4, %rax
	jle	.L56

	MOVDDUP( 0 * SIZE, X, %xmm0)
	MOVDDUP( 1 * SIZE, X, %xmm1)
	addq	INCX, X
	MOVDDUP( 0 * SIZE, X, %xmm2)
	MOVDDUP( 1 * SIZE, X, %xmm3)
	addq	INCX, X

	MOVDDUP( 0 * SIZE, X, %xmm4)
	MOVDDUP( 1 * SIZE, X, %xmm5)
	addq	INCX, X
	MOVDDUP( 0 * SIZE, X, %xmm6)
	MOVDDUP( 1 * SIZE, X, %xmm7)
	addq	INCX, X

	movsd	 0 * SIZE(Y), %xmm8
	movhpd	 1 * SIZE(Y), %xmm8
	addq	 INCY, Y
	movsd	 0 * SIZE(Y), %xmm9
	movhpd	 1 * SIZE(Y), %xmm9
	addq	 INCY, Y
	movsd	 0 * SIZE(Y), %xmm10
	movhpd	 1 * SIZE(Y), %xmm10
	addq	 INCY, Y
	movsd	 0 * SIZE(Y), %xmm11
	movhpd	 1 * SIZE(Y), %xmm11
	addq	 INCY, Y

	mulpd	%xmm14, %xmm0
	mulpd	%xmm14, %xmm2
	mulpd	%xmm14, %xmm4
	mulpd	%xmm14, %xmm6

	addpd	%xmm0, %xmm8
	mulpd	%xmm15, %xmm1
	addpd	%xmm2, %xmm9
	mulpd	%xmm15, %xmm3
	addpd	%xmm4, %xmm10
	mulpd	%xmm15, %xmm5
	addpd	%xmm6, %xmm11
	mulpd	%xmm15, %xmm7

	addpd	%xmm1, %xmm8
	addpd	%xmm3, %xmm9
	addpd	%xmm5, %xmm10
	addpd	%xmm7, %xmm11

	movlpd	%xmm8,  0 * SIZE(YY)
	movhpd	%xmm8,  1 * SIZE(YY)
	addq	 INCY, YY
	movlpd	%xmm9,  0 * SIZE(YY)
	movhpd	%xmm9,  1 * SIZE(YY)
	addq	 INCY, YY
	movlpd	%xmm10, 0 * SIZE(YY)
	movhpd	%xmm10, 1 * SIZE(YY)
	addq	 INCY, YY
	movlpd	%xmm11, 0 * SIZE(YY)
	movhpd	%xmm11, 1 * SIZE(YY)
	addq	 INCY, YY
	ALIGN_3

.L56:
	movq	M,  %rax
	andq	$2, %rax
	jle	.L57

	MOVDDUP( 0 * SIZE, X, %xmm0)
	MOVDDUP( 1 * SIZE, X, %xmm1)
	addq	INCX, X
	MOVDDUP( 0 * SIZE, X, %xmm2)
	MOVDDUP( 1 * SIZE, X, %xmm3)
	addq	INCX, X

	movsd	 0 * SIZE(Y), %xmm8
	movhpd	 1 * SIZE(Y), %xmm8
	addq	 INCY, Y
	movsd	 0 * SIZE(Y), %xmm9
	movhpd	 1 * SIZE(Y), %xmm9
	addq	 INCY, Y

	mulpd	%xmm14, %xmm0
	mulpd	%xmm14, %xmm2
	mulpd	%xmm15, %xmm1
	mulpd	%xmm15, %xmm3

	addpd	%xmm0, %xmm8
	addpd	%xmm2, %xmm9
	addpd	%xmm1, %xmm8
	addpd	%xmm3, %xmm9

	movlpd	%xmm8,   0 * SIZE(YY)
	movhpd	%xmm8,   1 * SIZE(YY)
	addq	 INCY, YY
	movlpd	%xmm9,   0 * SIZE(YY)
	movhpd	%xmm9,   1 * SIZE(YY)
	addq	 INCY, YY
	ALIGN_3

.L57:
	movq	M,  %rax
	andq	$1, %rax
	jle	.L999

.L58:
	MOVDDUP( 0 * SIZE, X, %xmm0)
	MOVDDUP( 1 * SIZE, X, %xmm1)

	movsd	 0 * SIZE(Y), %xmm8
	movhpd	 1 * SIZE(Y), %xmm8
	mulpd	%xmm14, %xmm0
	mulpd	%xmm15, %xmm1
	addpd	%xmm0, %xmm8
	addpd	%xmm1, %xmm8

	movlpd	%xmm8,   0 * SIZE(YY)
	movhpd	%xmm8,   1 * SIZE(YY)

	decq %rax
	jg	.L58
	ALIGN_3

.L999:
	xorq	%rax, %rax

	RESTOREREGISTERS

	ret

	EPILOGUE
