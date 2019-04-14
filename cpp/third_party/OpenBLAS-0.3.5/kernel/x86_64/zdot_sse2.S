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
#define N	ARG1	/* rdi */
#define X	ARG2	/* rsi */
#define INCX	ARG3	/* rdx */
#define Y	ARG4	/* rcx */
#define INCY	ARG5	/* r8  */
#else
#define RESULT_ADDRESS	ARG1    /*rcx*/
#define N	ARG2	/* rdx */
#define X	ARG3	/* r8 */
#define INCX	ARG4	/* r9*/
#define Y	%r10
#define INCY	%r11
#endif

#include "l1param.h"

#undef movsd

#ifndef OPTERON
#define MOVLPS	movsd
#else
#define MOVLPS	movlps
#endif


	PROLOGUE
	PROFCODE

#ifdef WINDOWS_ABI
	movq	40(%rsp), Y
	movq	48(%rsp), INCY
#endif

	SAVEREGISTERS

	salq	$ZBASE_SHIFT, INCX
	salq	$ZBASE_SHIFT, INCY

	xorps	%xmm0, %xmm0
	xorps	%xmm1, %xmm1
	xorps	%xmm2, %xmm2
	xorps	%xmm3, %xmm3

	cmpq	$0, N
	jle	.L999

	cmpq	$2 * SIZE, INCX
	jne	.L50
	cmpq	$2 * SIZE, INCY
	jne	.L50

	subq	$-16 * SIZE, X
	subq	$-16 * SIZE, Y

	testq	$SIZE, Y
	jne	.L30

	testq	$SIZE, X
	jne	.L20

	movq	N,  %rax
	sarq	$3, %rax
	jle	.L15

	movaps	-16 * SIZE(X), %xmm4
	movaps	-14 * SIZE(X), %xmm5
	movaps	-16 * SIZE(Y), %xmm8
	movaps	-14 * SIZE(Y), %xmm9
	movaps	-12 * SIZE(X), %xmm6
	movaps	-10 * SIZE(X), %xmm7
	movaps	-12 * SIZE(Y), %xmm10
	movaps	-10 * SIZE(Y), %xmm11

	decq	%rax
	jle	.L12
	ALIGN_3

.L11:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	pshufd	$0x4e,  %xmm8, %xmm12
	mulpd	%xmm4,  %xmm8
	addpd	%xmm8,  %xmm0
	movaps	 -8 * SIZE(Y), %xmm8
	mulpd	%xmm4,  %xmm12
	movaps	 -8 * SIZE(X), %xmm4
	addpd	%xmm12, %xmm1

	pshufd	$0x4e,  %xmm9, %xmm12
	mulpd	%xmm5,  %xmm9
	addpd	%xmm9,  %xmm2
	movaps	 -6 * SIZE(Y), %xmm9
	mulpd	%xmm5,  %xmm12
	movaps	 -6 * SIZE(X), %xmm5
	addpd	%xmm12, %xmm3

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	pshufd	$0x4e,  %xmm10, %xmm12
	mulpd	%xmm6,  %xmm10
	addpd	%xmm10, %xmm0
	movaps	 -4 * SIZE(Y), %xmm10
	mulpd	%xmm6,  %xmm12
	movaps	 -4 * SIZE(X), %xmm6
	addpd	%xmm12, %xmm1

	pshufd	$0x4e,  %xmm11, %xmm12
	mulpd	%xmm7,  %xmm11
	addpd	%xmm11, %xmm2
	movaps	 -2 * SIZE(Y), %xmm11
	mulpd	%xmm7,  %xmm12
	movaps	 -2 * SIZE(X), %xmm7
	addpd	%xmm12, %xmm3

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	pshufd	$0x4e,  %xmm8, %xmm12
	mulpd	%xmm4,  %xmm8
	addpd	%xmm8,  %xmm0
	movaps	  0 * SIZE(Y), %xmm8
	mulpd	%xmm4,  %xmm12
	movaps	  0 * SIZE(X), %xmm4
	addpd	%xmm12, %xmm1

	pshufd	$0x4e,  %xmm9, %xmm12
	mulpd	%xmm5,  %xmm9
	addpd	%xmm9,  %xmm2
	movaps	  2 * SIZE(Y), %xmm9
	mulpd	%xmm5,  %xmm12
	movaps	  2 * SIZE(X), %xmm5
	addpd	%xmm12, %xmm3

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	pshufd	$0x4e,  %xmm10, %xmm12
	mulpd	%xmm6,  %xmm10
	addpd	%xmm10, %xmm0
	movaps	  4 * SIZE(Y), %xmm10
	mulpd	%xmm6,  %xmm12
	movaps	  4 * SIZE(X), %xmm6
	addpd	%xmm12, %xmm1

	pshufd	$0x4e,  %xmm11, %xmm12
	mulpd	%xmm7,  %xmm11
	addpd	%xmm11, %xmm2
	movaps	  6 * SIZE(Y), %xmm11
	mulpd	%xmm7,  %xmm12
	movaps	  6 * SIZE(X), %xmm7
	addpd	%xmm12, %xmm3

	subq	$-16 * SIZE, X
	subq	$-16 * SIZE, Y

	decq	%rax
	jg	.L11
	ALIGN_3

.L12:
	pshufd	$0x4e,  %xmm8, %xmm12
	mulpd	%xmm4,  %xmm8
	addpd	%xmm8,  %xmm0
	movaps	 -8 * SIZE(Y), %xmm8
	mulpd	%xmm4,  %xmm12
	movaps	 -8 * SIZE(X), %xmm4
	addpd	%xmm12, %xmm1

	pshufd	$0x4e,  %xmm9, %xmm12
	mulpd	%xmm5,  %xmm9
	addpd	%xmm9,  %xmm2
	movaps	 -6 * SIZE(Y), %xmm9
	mulpd	%xmm5,  %xmm12
	movaps	 -6 * SIZE(X), %xmm5
	addpd	%xmm12, %xmm3

	pshufd	$0x4e,  %xmm10, %xmm12
	mulpd	%xmm6,  %xmm10
	addpd	%xmm10, %xmm0
	movaps	 -4 * SIZE(Y), %xmm10
	mulpd	%xmm6,  %xmm12
	movaps	 -4 * SIZE(X), %xmm6
	addpd	%xmm12, %xmm1

	pshufd	$0x4e,  %xmm11, %xmm12
	mulpd	%xmm7,  %xmm11
	addpd	%xmm11, %xmm2
	movaps	 -2 * SIZE(Y), %xmm11
	mulpd	%xmm7,  %xmm12
	movaps	 -2 * SIZE(X), %xmm7
	addpd	%xmm12, %xmm3

	pshufd	$0x4e,  %xmm8, %xmm12
	mulpd	%xmm4,  %xmm8
	addpd	%xmm8,  %xmm0
	mulpd	%xmm4,  %xmm12
	addpd	%xmm12, %xmm1

	pshufd	$0x4e,  %xmm9, %xmm12
	mulpd	%xmm5,  %xmm9
	addpd	%xmm9,  %xmm2
	mulpd	%xmm5,  %xmm12
	addpd	%xmm12, %xmm3

	pshufd	$0x4e,  %xmm10, %xmm12
	mulpd	%xmm6,  %xmm10
	addpd	%xmm10, %xmm0
	mulpd	%xmm6,  %xmm12
	addpd	%xmm12, %xmm1

	pshufd	$0x4e,  %xmm11, %xmm12
	mulpd	%xmm7,  %xmm11
	addpd	%xmm11, %xmm2
	mulpd	%xmm7,  %xmm12
	addpd	%xmm12, %xmm3

	subq	$-16 * SIZE, X
	subq	$-16 * SIZE, Y
	ALIGN_3

.L15:
	testq	$4, N
	jle	.L16

	movaps	-16 * SIZE(X), %xmm4
	movaps	-16 * SIZE(Y), %xmm8
	movaps	-14 * SIZE(X), %xmm5
	movaps	-14 * SIZE(Y), %xmm9

	pshufd	$0x4e,  %xmm8, %xmm12
	mulpd	%xmm4,  %xmm8
	addpd	%xmm8,  %xmm0
	mulpd	%xmm4,  %xmm12
	addpd	%xmm12, %xmm1

	pshufd	$0x4e,  %xmm9, %xmm12
	mulpd	%xmm5,  %xmm9
	addpd	%xmm9,  %xmm2
	mulpd	%xmm5,  %xmm12
	addpd	%xmm12, %xmm3

	movaps	-12 * SIZE(X), %xmm6
	movaps	-12 * SIZE(Y), %xmm10
	movaps	-10 * SIZE(X), %xmm7
	movaps	-10 * SIZE(Y), %xmm11

	pshufd	$0x4e,  %xmm10, %xmm12
	mulpd	%xmm6,  %xmm10
	addpd	%xmm10, %xmm0
	mulpd	%xmm6,  %xmm12
	addpd	%xmm12, %xmm1

	pshufd	$0x4e,  %xmm11, %xmm12
	mulpd	%xmm7,  %xmm11
	addpd	%xmm11, %xmm2
	mulpd	%xmm7,  %xmm12
	addpd	%xmm12, %xmm3

	addq	$8 * SIZE, X
	addq	$8 * SIZE, Y
	ALIGN_3

.L16:
	testq	$2, N
	jle	.L17

	movaps	-16 * SIZE(X), %xmm4
	movaps	-16 * SIZE(Y), %xmm8
	movaps	-14 * SIZE(X), %xmm5
	movaps	-14 * SIZE(Y), %xmm9

	pshufd	$0x4e,  %xmm8, %xmm12
	mulpd	%xmm4,  %xmm8
	addpd	%xmm8,  %xmm0
	mulpd	%xmm4,  %xmm12
	addpd	%xmm12, %xmm1

	pshufd	$0x4e,  %xmm9, %xmm12
	mulpd	%xmm5,  %xmm9
	addpd	%xmm9,  %xmm2
	mulpd	%xmm5,  %xmm12
	addpd	%xmm12, %xmm3

	addq	$4 * SIZE, X
	addq	$4 * SIZE, Y
	ALIGN_3

.L17:
	testq	$1, N
	jle	.L98

	movaps	-16 * SIZE(X), %xmm4
	movaps	-16 * SIZE(Y), %xmm8

	pshufd	$0x4e,  %xmm8, %xmm12
	mulpd	%xmm4,  %xmm8
	addpd	%xmm8,  %xmm0
	mulpd	%xmm4,  %xmm12
	addpd	%xmm12, %xmm1
	jmp	.L98
	ALIGN_3

.L20:
	movq	N,  %rax
	sarq	$3, %rax
	jle	.L25

	MOVLPS	-16 * SIZE(X), %xmm4
	movhps	-15 * SIZE(X), %xmm4
	MOVLPS	-14 * SIZE(X), %xmm5
	movhps	-13 * SIZE(X), %xmm5
	movaps	-16 * SIZE(Y), %xmm8
	movaps	-14 * SIZE(Y), %xmm9
	MOVLPS	-12 * SIZE(X), %xmm6
	movhps	-11 * SIZE(X), %xmm6
	MOVLPS	-10 * SIZE(X), %xmm7
	movhps	 -9 * SIZE(X), %xmm7
	movaps	-12 * SIZE(Y), %xmm10
	movaps	-10 * SIZE(Y), %xmm11

	decq	%rax
	jle	.L22
	ALIGN_3

.L21:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	pshufd	$0x4e,  %xmm8, %xmm12
	mulpd	%xmm4,  %xmm8
	addpd	%xmm8,  %xmm0
	movaps	 -8 * SIZE(Y), %xmm8
	mulpd	%xmm4,  %xmm12
	MOVLPS	 -8 * SIZE(X), %xmm4
	movhps	 -7 * SIZE(X), %xmm4
	addpd	%xmm12, %xmm1

	pshufd	$0x4e,  %xmm9, %xmm12
	mulpd	%xmm5,  %xmm9
	addpd	%xmm9,  %xmm2
	movaps	 -6 * SIZE(Y), %xmm9
	mulpd	%xmm5,  %xmm12
	MOVLPS	 -6 * SIZE(X), %xmm5
	movhps	 -5 * SIZE(X), %xmm5
	addpd	%xmm12, %xmm3

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	pshufd	$0x4e,  %xmm10, %xmm12
	mulpd	%xmm6,  %xmm10
	addpd	%xmm10, %xmm0
	movaps	 -4 * SIZE(Y), %xmm10
	mulpd	%xmm6,  %xmm12
	MOVLPS	 -4 * SIZE(X), %xmm6
	movhps	 -3 * SIZE(X), %xmm6
	addpd	%xmm12, %xmm1

	pshufd	$0x4e,  %xmm11, %xmm12
	mulpd	%xmm7,  %xmm11
	addpd	%xmm11, %xmm2
	movaps	 -2 * SIZE(Y), %xmm11
	mulpd	%xmm7,  %xmm12
	MOVLPS	 -2 * SIZE(X), %xmm7
	movhps	 -1 * SIZE(X), %xmm7
	addpd	%xmm12, %xmm3

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	pshufd	$0x4e,  %xmm8, %xmm12
	mulpd	%xmm4,  %xmm8
	addpd	%xmm8,  %xmm0
	movaps	  0 * SIZE(Y), %xmm8
	mulpd	%xmm4,  %xmm12
	MOVLPS	  0 * SIZE(X), %xmm4
	movhps	  1 * SIZE(X), %xmm4
	addpd	%xmm12, %xmm1

	pshufd	$0x4e,  %xmm9, %xmm12
	mulpd	%xmm5,  %xmm9
	addpd	%xmm9,  %xmm2
	movaps	  2 * SIZE(Y), %xmm9
	mulpd	%xmm5,  %xmm12
	MOVLPS	  2 * SIZE(X), %xmm5
	movhps	  3 * SIZE(X), %xmm5
	addpd	%xmm12, %xmm3

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	pshufd	$0x4e,  %xmm10, %xmm12
	mulpd	%xmm6,  %xmm10
	addpd	%xmm10, %xmm0
	movaps	  4 * SIZE(Y), %xmm10
	mulpd	%xmm6,  %xmm12
	MOVLPS	  4 * SIZE(X), %xmm6
	movhps	  5 * SIZE(X), %xmm6
	addpd	%xmm12, %xmm1

	pshufd	$0x4e,  %xmm11, %xmm12
	mulpd	%xmm7,  %xmm11
	addpd	%xmm11, %xmm2
	movaps	  6 * SIZE(Y), %xmm11
	mulpd	%xmm7,  %xmm12
	MOVLPS	  6 * SIZE(X), %xmm7
	movhps	  7 * SIZE(X), %xmm7
	addpd	%xmm12, %xmm3

	subq	$-16 * SIZE, X
	subq	$-16 * SIZE, Y

	decq	%rax
	jg	.L21
	ALIGN_3

.L22:

	pshufd	$0x4e,  %xmm8, %xmm12
	mulpd	%xmm4,  %xmm8
	addpd	%xmm8,  %xmm0
	movaps	 -8 * SIZE(Y), %xmm8
	mulpd	%xmm4,  %xmm12
	MOVLPS	 -8 * SIZE(X), %xmm4
	movhps	 -7 * SIZE(X), %xmm4
	addpd	%xmm12, %xmm1

	pshufd	$0x4e,  %xmm9, %xmm12
	mulpd	%xmm5,  %xmm9
	addpd	%xmm9,  %xmm2
	movaps	 -6 * SIZE(Y), %xmm9
	mulpd	%xmm5,  %xmm12
	MOVLPS	 -6 * SIZE(X), %xmm5
	movhps	 -5 * SIZE(X), %xmm5
	addpd	%xmm12, %xmm3

	pshufd	$0x4e,  %xmm10, %xmm12
	mulpd	%xmm6,  %xmm10
	addpd	%xmm10, %xmm0
	movaps	 -4 * SIZE(Y), %xmm10
	mulpd	%xmm6,  %xmm12
	MOVLPS	 -4 * SIZE(X), %xmm6
	movhps	 -3 * SIZE(X), %xmm6
	addpd	%xmm12, %xmm1

	pshufd	$0x4e,  %xmm11, %xmm12
	mulpd	%xmm7,  %xmm11
	addpd	%xmm11, %xmm2
	movaps	 -2 * SIZE(Y), %xmm11
	mulpd	%xmm7,  %xmm12
	MOVLPS	 -2 * SIZE(X), %xmm7
	movhps	 -1 * SIZE(X), %xmm7
	addpd	%xmm12, %xmm3

	pshufd	$0x4e,  %xmm8, %xmm12
	mulpd	%xmm4,  %xmm8
	addpd	%xmm8,  %xmm0
	mulpd	%xmm4,  %xmm12
	addpd	%xmm12, %xmm1

	pshufd	$0x4e,  %xmm9, %xmm12
	mulpd	%xmm5,  %xmm9
	addpd	%xmm9,  %xmm2
	mulpd	%xmm5,  %xmm12
	addpd	%xmm12, %xmm3

	pshufd	$0x4e,  %xmm10, %xmm12
	mulpd	%xmm6,  %xmm10
	addpd	%xmm10, %xmm0
	mulpd	%xmm6,  %xmm12
	addpd	%xmm12, %xmm1

	pshufd	$0x4e,  %xmm11, %xmm12
	mulpd	%xmm7,  %xmm11
	addpd	%xmm11, %xmm2
	mulpd	%xmm7,  %xmm12
	addpd	%xmm12, %xmm3

	subq	$-16 * SIZE, X
	subq	$-16 * SIZE, Y
	ALIGN_3

.L25:
	testq	$4, N
	jle	.L26

	MOVLPS	-16 * SIZE(X), %xmm4
	movhps	-15 * SIZE(X), %xmm4
	movaps	-16 * SIZE(Y), %xmm8

	pshufd	$0x4e,  %xmm8, %xmm12
	mulpd	%xmm4,  %xmm8
	addpd	%xmm8,  %xmm0
	mulpd	%xmm4,  %xmm12
	addpd	%xmm12, %xmm1

	MOVLPS	-14 * SIZE(X), %xmm5
	movhps	-13 * SIZE(X), %xmm5
	movaps	-14 * SIZE(Y), %xmm9

	pshufd	$0x4e,  %xmm9, %xmm12
	mulpd	%xmm5,  %xmm9
	addpd	%xmm9,  %xmm2
	mulpd	%xmm5,  %xmm12
	addpd	%xmm12, %xmm3

	MOVLPS	-12 * SIZE(X), %xmm6
	movhps	-11 * SIZE(X), %xmm6
	movaps	-12 * SIZE(Y), %xmm10

	pshufd	$0x4e,  %xmm10, %xmm12
	mulpd	%xmm6,  %xmm10
	addpd	%xmm10, %xmm0
	mulpd	%xmm6,  %xmm12
	addpd	%xmm12, %xmm1

	MOVLPS	-10 * SIZE(X), %xmm7
	movhps	 -9 * SIZE(X), %xmm7
	movaps	-10 * SIZE(Y), %xmm11

	pshufd	$0x4e,  %xmm11, %xmm12
	mulpd	%xmm7,  %xmm11
	addpd	%xmm11, %xmm2
	mulpd	%xmm7,  %xmm12
	addpd	%xmm12, %xmm3

	addq	$8 * SIZE, X
	addq	$8 * SIZE, Y
	ALIGN_3

.L26:
	testq	$2, N
	jle	.L27

	MOVLPS	-16 * SIZE(X), %xmm4
	movhps	-15 * SIZE(X), %xmm4
	movaps	-16 * SIZE(Y), %xmm8

	pshufd	$0x4e,  %xmm8, %xmm12
	mulpd	%xmm4,  %xmm8
	addpd	%xmm8,  %xmm0
	mulpd	%xmm4,  %xmm12
	addpd	%xmm12, %xmm1

	MOVLPS	-14 * SIZE(X), %xmm5
	movhps	-13 * SIZE(X), %xmm5
	movaps	-14 * SIZE(Y), %xmm9

	pshufd	$0x4e,  %xmm9, %xmm12
	mulpd	%xmm5,  %xmm9
	addpd	%xmm9,  %xmm2
	mulpd	%xmm5,  %xmm12
	addpd	%xmm12, %xmm3

	addq	$4 * SIZE, X
	addq	$4 * SIZE, Y
	ALIGN_3

.L27:
	testq	$1, N
	jle	.L98

	MOVLPS	-16 * SIZE(X), %xmm4
	movhps	-15 * SIZE(X), %xmm4
	movaps	-16 * SIZE(Y), %xmm8

	pshufd	$0x4e,  %xmm8, %xmm12
	mulpd	%xmm4,  %xmm8
	addpd	%xmm8,  %xmm0
	mulpd	%xmm4,  %xmm12
	addpd	%xmm12, %xmm1
	jmp	.L98
	ALIGN_3

.L30:
	testq	$SIZE, X
	jne	.L40

	movq	N,  %rax
	sarq	$3, %rax
	jle	.L35

	MOVLPS	-16 * SIZE(Y), %xmm4
	movhps	-15 * SIZE(Y), %xmm4
	MOVLPS	-14 * SIZE(Y), %xmm5
	movhps	-13 * SIZE(Y), %xmm5
	movaps	-16 * SIZE(X), %xmm8
	movaps	-14 * SIZE(X), %xmm9
	MOVLPS	-12 * SIZE(Y), %xmm6
	movhps	-11 * SIZE(Y), %xmm6
	MOVLPS	-10 * SIZE(Y), %xmm7
	movhps	 -9 * SIZE(Y), %xmm7
	movaps	-12 * SIZE(X), %xmm10
	movaps	-10 * SIZE(X), %xmm11

	decq	%rax
	jle	.L32
	ALIGN_3

.L31:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	pshufd	$0x4e,  %xmm8, %xmm12
	mulpd	%xmm4,  %xmm8
	addpd	%xmm8,  %xmm0
	movaps	 -8 * SIZE(X), %xmm8
	mulpd	%xmm4,  %xmm12
	MOVLPS	 -8 * SIZE(Y), %xmm4
	movhps	 -7 * SIZE(Y), %xmm4
	addpd	%xmm12, %xmm1

	pshufd	$0x4e,  %xmm9, %xmm12
	mulpd	%xmm5,  %xmm9
	addpd	%xmm9,  %xmm2
	movaps	 -6 * SIZE(X), %xmm9
	mulpd	%xmm5,  %xmm12
	MOVLPS	 -6 * SIZE(Y), %xmm5
	movhps	 -5 * SIZE(Y), %xmm5
	addpd	%xmm12, %xmm3

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	pshufd	$0x4e,  %xmm10, %xmm12
	mulpd	%xmm6,  %xmm10
	addpd	%xmm10, %xmm0
	movaps	 -4 * SIZE(X), %xmm10
	mulpd	%xmm6,  %xmm12
	MOVLPS	 -4 * SIZE(Y), %xmm6
	movhps	 -3 * SIZE(Y), %xmm6
	addpd	%xmm12, %xmm1

	pshufd	$0x4e,  %xmm11, %xmm12
	mulpd	%xmm7,  %xmm11
	addpd	%xmm11, %xmm2
	movaps	 -2 * SIZE(X), %xmm11
	mulpd	%xmm7,  %xmm12
	MOVLPS	 -2 * SIZE(Y), %xmm7
	movhps	 -1 * SIZE(Y), %xmm7
	addpd	%xmm12, %xmm3

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	pshufd	$0x4e,  %xmm8, %xmm12
	mulpd	%xmm4,  %xmm8
	addpd	%xmm8,  %xmm0
	movaps	  0 * SIZE(X), %xmm8
	mulpd	%xmm4,  %xmm12
	MOVLPS	  0 * SIZE(Y), %xmm4
	movhps	  1 * SIZE(Y), %xmm4
	addpd	%xmm12, %xmm1

	pshufd	$0x4e,  %xmm9, %xmm12
	mulpd	%xmm5,  %xmm9
	addpd	%xmm9,  %xmm2
	movaps	  2 * SIZE(X), %xmm9
	mulpd	%xmm5,  %xmm12
	MOVLPS	  2 * SIZE(Y), %xmm5
	movhps	  3 * SIZE(Y), %xmm5
	addpd	%xmm12, %xmm3

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	pshufd	$0x4e,  %xmm10, %xmm12
	mulpd	%xmm6,  %xmm10
	addpd	%xmm10, %xmm0
	movaps	  4 * SIZE(X), %xmm10
	mulpd	%xmm6,  %xmm12
	MOVLPS	  4 * SIZE(Y), %xmm6
	movhps	  5 * SIZE(Y), %xmm6
	addpd	%xmm12, %xmm1

	pshufd	$0x4e,  %xmm11, %xmm12
	mulpd	%xmm7,  %xmm11
	addpd	%xmm11, %xmm2
	movaps	  6 * SIZE(X), %xmm11
	mulpd	%xmm7,  %xmm12
	MOVLPS	  6 * SIZE(Y), %xmm7
	movhps	  7 * SIZE(Y), %xmm7
	addpd	%xmm12, %xmm3

	subq	$-16 * SIZE, X
	subq	$-16 * SIZE, Y

	decq	%rax
	jg	.L31
	ALIGN_3

.L32:

	pshufd	$0x4e,  %xmm8, %xmm12
	mulpd	%xmm4,  %xmm8
	addpd	%xmm8,  %xmm0
	movaps	 -8 * SIZE(X), %xmm8
	mulpd	%xmm4,  %xmm12
	MOVLPS	 -8 * SIZE(Y), %xmm4
	movhps	 -7 * SIZE(Y), %xmm4
	addpd	%xmm12, %xmm1

	pshufd	$0x4e,  %xmm9, %xmm12
	mulpd	%xmm5,  %xmm9
	addpd	%xmm9,  %xmm2
	movaps	 -6 * SIZE(X), %xmm9
	mulpd	%xmm5,  %xmm12
	MOVLPS	 -6 * SIZE(Y), %xmm5
	movhps	 -5 * SIZE(Y), %xmm5
	addpd	%xmm12, %xmm3

	pshufd	$0x4e,  %xmm10, %xmm12
	mulpd	%xmm6,  %xmm10
	addpd	%xmm10, %xmm0
	movaps	 -4 * SIZE(X), %xmm10
	mulpd	%xmm6,  %xmm12
	MOVLPS	 -4 * SIZE(Y), %xmm6
	movhps	 -3 * SIZE(Y), %xmm6
	addpd	%xmm12, %xmm1

	pshufd	$0x4e,  %xmm11, %xmm12
	mulpd	%xmm7,  %xmm11
	addpd	%xmm11, %xmm2
	movaps	 -2 * SIZE(X), %xmm11
	mulpd	%xmm7,  %xmm12
	MOVLPS	 -2 * SIZE(Y), %xmm7
	movhps	 -1 * SIZE(Y), %xmm7
	addpd	%xmm12, %xmm3

	pshufd	$0x4e,  %xmm8, %xmm12
	mulpd	%xmm4,  %xmm8
	addpd	%xmm8,  %xmm0
	mulpd	%xmm4,  %xmm12
	addpd	%xmm12, %xmm1

	pshufd	$0x4e,  %xmm9, %xmm12
	mulpd	%xmm5,  %xmm9
	addpd	%xmm9,  %xmm2
	mulpd	%xmm5,  %xmm12
	addpd	%xmm12, %xmm3

	pshufd	$0x4e,  %xmm10, %xmm12
	mulpd	%xmm6,  %xmm10
	addpd	%xmm10, %xmm0
	mulpd	%xmm6,  %xmm12
	addpd	%xmm12, %xmm1

	pshufd	$0x4e,  %xmm11, %xmm12
	mulpd	%xmm7,  %xmm11
	addpd	%xmm11, %xmm2
	mulpd	%xmm7,  %xmm12
	addpd	%xmm12, %xmm3

	subq	$-16 * SIZE, X
	subq	$-16 * SIZE, Y
	ALIGN_3

.L35:
	testq	$4, N
	jle	.L36

	MOVLPS	-16 * SIZE(Y), %xmm4
	movhps	-15 * SIZE(Y), %xmm4
	movaps	-16 * SIZE(X), %xmm8

	pshufd	$0x4e,  %xmm8, %xmm12
	mulpd	%xmm4,  %xmm8
	addpd	%xmm8,  %xmm0
	mulpd	%xmm4,  %xmm12
	addpd	%xmm12, %xmm1

	MOVLPS	-14 * SIZE(Y), %xmm5
	movhps	-13 * SIZE(Y), %xmm5
	movaps	-14 * SIZE(X), %xmm9

	pshufd	$0x4e,  %xmm9, %xmm12
	mulpd	%xmm5,  %xmm9
	addpd	%xmm9,  %xmm2
	mulpd	%xmm5,  %xmm12
	addpd	%xmm12, %xmm3

	MOVLPS	-12 * SIZE(Y), %xmm6
	movhps	-11 * SIZE(Y), %xmm6
	movaps	-12 * SIZE(X), %xmm10

	pshufd	$0x4e,  %xmm10, %xmm12
	mulpd	%xmm6,  %xmm10
	addpd	%xmm10, %xmm0
	mulpd	%xmm6,  %xmm12
	addpd	%xmm12, %xmm1

	MOVLPS	-10 * SIZE(Y), %xmm7
	movhps	 -9 * SIZE(Y), %xmm7
	movaps	-10 * SIZE(X), %xmm11

	pshufd	$0x4e,  %xmm11, %xmm12
	mulpd	%xmm7,  %xmm11
	addpd	%xmm11, %xmm2
	mulpd	%xmm7,  %xmm12
	addpd	%xmm12, %xmm3

	addq	$8 * SIZE, X
	addq	$8 * SIZE, Y
	ALIGN_3

.L36:
	testq	$2, N
	jle	.L37

	MOVLPS	-16 * SIZE(Y), %xmm4
	movhps	-15 * SIZE(Y), %xmm4
	movaps	-16 * SIZE(X), %xmm8

	pshufd	$0x4e,  %xmm8, %xmm12
	mulpd	%xmm4,  %xmm8
	addpd	%xmm8,  %xmm0
	mulpd	%xmm4,  %xmm12
	addpd	%xmm12, %xmm1

	MOVLPS	-14 * SIZE(Y), %xmm5
	movhps	-13 * SIZE(Y), %xmm5
	movaps	-14 * SIZE(X), %xmm9

	pshufd	$0x4e,  %xmm9, %xmm12
	mulpd	%xmm5,  %xmm9
	addpd	%xmm9,  %xmm2
	mulpd	%xmm5,  %xmm12
	addpd	%xmm12, %xmm3

	addq	$4 * SIZE, X
	addq	$4 * SIZE, Y
	ALIGN_3

.L37:
	SHUFPD_1 %xmm1, %xmm1
	SHUFPD_1 %xmm3, %xmm3

	testq	$1, N
	jle	.L98

	MOVLPS	-16 * SIZE(Y), %xmm4
	movhps	-15 * SIZE(Y), %xmm4
	movaps	-16 * SIZE(X), %xmm8

	pshufd	$0x4e,  %xmm8, %xmm12
	mulpd	%xmm4,  %xmm8
	addpd	%xmm8,  %xmm0
	mulpd	%xmm4,  %xmm12
	SHUFPD_1 %xmm12, %xmm12
	addpd	%xmm12, %xmm1
	jmp	.L98
	ALIGN_3

.L40:
	movhps	-16 * SIZE(X), %xmm4
	addq	$SIZE, X
	movhps	-16 * SIZE(Y), %xmm8
	addq	$SIZE, Y

	movq	N,  %rax
	sarq	$3, %rax
	jle	.L45

	movaps	-16 * SIZE(X), %xmm5
	movaps	-16 * SIZE(Y), %xmm9
	movaps	-14 * SIZE(X), %xmm6
	movaps	-14 * SIZE(Y), %xmm10
	movaps	-12 * SIZE(X), %xmm7
	movaps	-12 * SIZE(Y), %xmm11
	decq	%rax
	jle	.L42
	ALIGN_3

.L41:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	movsd	%xmm9,  %xmm8
	pshufd	$0x4e,  %xmm8, %xmm12
	movsd	%xmm5,  %xmm4
	mulpd	%xmm4,  %xmm8
	addpd	%xmm8,  %xmm0
	movaps	-10 * SIZE(Y), %xmm8
	mulpd	%xmm4,  %xmm12
	movaps	-10 * SIZE(X), %xmm4
	addpd	%xmm12, %xmm1

	movsd	%xmm10, %xmm9
	pshufd	$0x4e,  %xmm9, %xmm12
	movsd	%xmm6,  %xmm5
	mulpd	%xmm5,  %xmm9
	addpd	%xmm9,  %xmm0
 	movaps	 -8 * SIZE(Y), %xmm9
	mulpd	%xmm5,  %xmm12
	movaps	 -8 * SIZE(X), %xmm5
	addpd	%xmm12, %xmm1

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movsd	%xmm11, %xmm10
	pshufd	$0x4e,  %xmm10, %xmm12
	movsd	%xmm7,  %xmm6
	mulpd	%xmm6,  %xmm10
	addpd	%xmm10, %xmm0
	movaps	 -6 * SIZE(Y), %xmm10
	mulpd	%xmm6,  %xmm12
	movaps	 -6 * SIZE(X), %xmm6
	addpd	%xmm12, %xmm1

	movsd	%xmm8, %xmm11
	pshufd	$0x4e,  %xmm11, %xmm12
	movsd	%xmm4,  %xmm7
	mulpd	%xmm7,  %xmm11
	addpd	%xmm11,  %xmm0
	movaps	 -4 * SIZE(Y), %xmm11
	mulpd	%xmm7,  %xmm12
	movaps	 -4 * SIZE(X), %xmm7
	addpd	%xmm12, %xmm1

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	movsd	%xmm9,  %xmm8
	pshufd	$0x4e,  %xmm8, %xmm12
	movsd	%xmm5,  %xmm4
	mulpd	%xmm4,  %xmm8
	addpd	%xmm8,  %xmm0
	movaps	 -2 * SIZE(Y), %xmm8
	mulpd	%xmm4,  %xmm12
	movaps	 -2 * SIZE(X), %xmm4
	addpd	%xmm12, %xmm1

	movsd	%xmm10, %xmm9
	pshufd	$0x4e,  %xmm9, %xmm12
	movsd	%xmm6,  %xmm5
	mulpd	%xmm5,  %xmm9
	addpd	%xmm9,  %xmm0
	movaps	  0 * SIZE(Y), %xmm9
	mulpd	%xmm5,  %xmm12
	movaps	  0 * SIZE(X), %xmm5
	addpd	%xmm12, %xmm1

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	movsd	%xmm11, %xmm10
	pshufd	$0x4e,  %xmm10, %xmm12
	movsd	%xmm7,  %xmm6
	mulpd	%xmm6,  %xmm10
	addpd	%xmm10, %xmm0
	movaps	  2 * SIZE(Y), %xmm10
	mulpd	%xmm6,  %xmm12
	movaps	  2 * SIZE(X), %xmm6
	addpd	%xmm12, %xmm1

	movsd	%xmm8, %xmm11
	pshufd	$0x4e,  %xmm11, %xmm12
	movsd	%xmm4,  %xmm7
	mulpd	%xmm7,  %xmm11
	addpd	%xmm11,  %xmm0
	movaps	  4 * SIZE(Y), %xmm11
	mulpd	%xmm7,  %xmm12
	movaps	  4 * SIZE(X), %xmm7
	addpd	%xmm12, %xmm1

	subq	$-16 * SIZE, X
	subq	$-16 * SIZE, Y

	decq	%rax
	jg	.L41
	ALIGN_3

.L42:
	movsd	%xmm9,  %xmm8
	pshufd	$0x4e,  %xmm8, %xmm12
	movsd	%xmm5,  %xmm4
	mulpd	%xmm4,  %xmm8
	addpd	%xmm8,  %xmm0
	movaps	-10 * SIZE(Y), %xmm8
	mulpd	%xmm4,  %xmm12
	movaps	-10 * SIZE(X), %xmm4
	addpd	%xmm12, %xmm1

	movsd	%xmm10, %xmm9
	pshufd	$0x4e,  %xmm9, %xmm12
	movsd	%xmm6,  %xmm5
	mulpd	%xmm5,  %xmm9
	addpd	%xmm9,  %xmm0
 	movaps	 -8 * SIZE(Y), %xmm9
	mulpd	%xmm5,  %xmm12
	movaps	 -8 * SIZE(X), %xmm5
	addpd	%xmm12, %xmm1

	movsd	%xmm11, %xmm10
	pshufd	$0x4e,  %xmm10, %xmm12
	movsd	%xmm7,  %xmm6
	mulpd	%xmm6,  %xmm10
	addpd	%xmm10, %xmm0
	movaps	 -6 * SIZE(Y), %xmm10
	mulpd	%xmm6,  %xmm12
	movaps	 -6 * SIZE(X), %xmm6
	addpd	%xmm12, %xmm1

	movsd	%xmm8, %xmm11
	pshufd	$0x4e,  %xmm11, %xmm12
	movsd	%xmm4,  %xmm7
	mulpd	%xmm7,  %xmm11
	addpd	%xmm11,  %xmm0
	movaps	 -4 * SIZE(Y), %xmm11
	mulpd	%xmm7,  %xmm12
	movaps	 -4 * SIZE(X), %xmm7
	addpd	%xmm12, %xmm1

	movsd	%xmm9,  %xmm8
	pshufd	$0x4e,  %xmm8, %xmm12
	movsd	%xmm5,  %xmm4
	mulpd	%xmm4,  %xmm8
	addpd	%xmm8,  %xmm0
	movaps	 -2 * SIZE(Y), %xmm8
	mulpd	%xmm4,  %xmm12
	movaps	 -2 * SIZE(X), %xmm4
	addpd	%xmm12, %xmm1

	movsd	%xmm10, %xmm9
	pshufd	$0x4e,  %xmm9, %xmm12
	movsd	%xmm6,  %xmm5
	mulpd	%xmm5,  %xmm9
	addpd	%xmm9,  %xmm0
	mulpd	%xmm5,  %xmm12
	addpd	%xmm12, %xmm1

	movsd	%xmm11, %xmm10
	pshufd	$0x4e,  %xmm10, %xmm12
	movsd	%xmm7,  %xmm6
	mulpd	%xmm6,  %xmm10
	addpd	%xmm10, %xmm0
	mulpd	%xmm6,  %xmm12
	addpd	%xmm12, %xmm1

	movsd	%xmm8, %xmm11
	pshufd	$0x4e,  %xmm11, %xmm12
	movsd	%xmm4,  %xmm7
	mulpd	%xmm7,  %xmm11
	addpd	%xmm11,  %xmm0
	mulpd	%xmm7,  %xmm12
	addpd	%xmm12, %xmm1

	subq	$-16 * SIZE, X
	subq	$-16 * SIZE, Y
	ALIGN_3

.L45:
	testq	$4, N
	jle	.L46

	movaps	-16 * SIZE(X), %xmm5
	movaps	-16 * SIZE(Y), %xmm9
	movaps	-14 * SIZE(X), %xmm6
	movaps	-14 * SIZE(Y), %xmm10

	movsd	%xmm9,  %xmm8
	pshufd	$0x4e,  %xmm8, %xmm12
	movsd	%xmm5,  %xmm4
	mulpd	%xmm4,  %xmm8
	addpd	%xmm8,  %xmm0
	mulpd	%xmm4,  %xmm12
	addpd	%xmm12, %xmm1

	movaps	-12 * SIZE(X), %xmm7
	movaps	-12 * SIZE(Y), %xmm11

	movsd	%xmm10, %xmm9
	pshufd	$0x4e,  %xmm9, %xmm12
	movsd	%xmm6,  %xmm5
	mulpd	%xmm5,  %xmm9
	addpd	%xmm9,  %xmm0
	mulpd	%xmm5,  %xmm12
	addpd	%xmm12, %xmm1

	movaps	-10 * SIZE(X), %xmm4
	movaps	-10 * SIZE(Y), %xmm8

	movsd	%xmm11, %xmm10
	pshufd	$0x4e,  %xmm10, %xmm12
	movsd	%xmm7,  %xmm6
	mulpd	%xmm6,  %xmm10
	addpd	%xmm10, %xmm0
	mulpd	%xmm6,  %xmm12
	addpd	%xmm12, %xmm1

	movsd	%xmm8, %xmm11
	pshufd	$0x4e,  %xmm11, %xmm12
	movsd	%xmm4,  %xmm7
	mulpd	%xmm7,  %xmm11
	addpd	%xmm11,  %xmm0
	mulpd	%xmm7,  %xmm12
	addpd	%xmm12, %xmm1

	addq	$8 * SIZE, X
	addq	$8 * SIZE, Y
	ALIGN_3

.L46:
	testq	$2, N
	jle	.L47

	movaps	-16 * SIZE(X), %xmm5
	movaps	-16 * SIZE(Y), %xmm9

	movsd	%xmm9,  %xmm8
	pshufd	$0x4e,  %xmm8, %xmm12
	movsd	%xmm5,  %xmm4
	mulpd	%xmm4,  %xmm8
	addpd	%xmm8,  %xmm0
	mulpd	%xmm4,  %xmm12
	addpd	%xmm12, %xmm1

	movaps	-14 * SIZE(X), %xmm6
	movaps	-14 * SIZE(Y), %xmm10

	movsd	%xmm10, %xmm9
	pshufd	$0x4e,  %xmm9, %xmm12
	movsd	%xmm6,  %xmm5
	mulpd	%xmm5,  %xmm9
	addpd	%xmm9,  %xmm0
	mulpd	%xmm5,  %xmm12
	addpd	%xmm12, %xmm1

	movaps	%xmm6,  %xmm4
	movaps	%xmm10, %xmm8

	addq	$4 * SIZE, X
	addq	$4 * SIZE, Y
	ALIGN_3

.L47:
	testq	$1, N
	jle	.L48

	movlps	-16 * SIZE(X), %xmm4
	movlps	-16 * SIZE(Y), %xmm8

	pshufd	$0x4e,  %xmm8, %xmm12
	mulpd	%xmm4,  %xmm8
	addpd	%xmm8,  %xmm0
	mulpd	%xmm4,  %xmm12
	addpd	%xmm12, %xmm1
	ALIGN_3

.L48:
	SHUFPD_1 %xmm0, %xmm0
	SHUFPD_1 %xmm1, %xmm1
	SHUFPD_1 %xmm2, %xmm2
	SHUFPD_1 %xmm3, %xmm3
	jmp	.L98
	ALIGN_3

.L50:
	movq	N,  %rax
	sarq	$3, %rax
	jle	.L55

	MOVLPS	0 * SIZE(X), %xmm4
	movhps	1 * SIZE(X), %xmm4
	addq	INCX, X
	MOVLPS	0 * SIZE(Y), %xmm8
	movhps	1 * SIZE(Y), %xmm8
	addq	INCY, Y

	MOVLPS	0 * SIZE(X), %xmm5
	movhps	1 * SIZE(X), %xmm5
	addq	INCX, X
	MOVLPS	0 * SIZE(Y), %xmm9
	movhps	1 * SIZE(Y), %xmm9
	addq	INCY, Y

	MOVLPS	0 * SIZE(X), %xmm6
	movhps	1 * SIZE(X), %xmm6
	addq	INCX, X
	MOVLPS	0 * SIZE(Y), %xmm10
	movhps	1 * SIZE(Y), %xmm10
	addq	INCY, Y

	MOVLPS	0 * SIZE(X), %xmm7
	movhps	1 * SIZE(X), %xmm7
	addq	INCX, X
	MOVLPS	0 * SIZE(Y), %xmm11
	movhps	1 * SIZE(Y), %xmm11
	addq	INCY, Y

	decq	%rax
	jle	.L54
	ALIGN_3

.L53:
	pshufd	$0x4e,  %xmm8, %xmm12
	mulpd	%xmm4,  %xmm8
	addpd	%xmm8,  %xmm0
	MOVLPS	0 * SIZE(Y), %xmm8
	movhps	1 * SIZE(Y), %xmm8
	addq	INCY, Y
	mulpd	%xmm4,  %xmm12
	MOVLPS	0 * SIZE(X), %xmm4
	movhps	1 * SIZE(X), %xmm4
	addq	INCX, X
	addpd	%xmm12, %xmm1

	pshufd	$0x4e,  %xmm9, %xmm12
	mulpd	%xmm5,  %xmm9
	addpd	%xmm9,  %xmm2
	MOVLPS	0 * SIZE(Y), %xmm9
	movhps	1 * SIZE(Y), %xmm9
	addq	INCY, Y
	mulpd	%xmm5,  %xmm12
	MOVLPS	0 * SIZE(X), %xmm5
	movhps	1 * SIZE(X), %xmm5
	addq	INCX, X
	addpd	%xmm12, %xmm3

	pshufd	$0x4e,  %xmm10, %xmm12
	mulpd	%xmm6,  %xmm10
	addpd	%xmm10, %xmm0
	MOVLPS	0 * SIZE(Y), %xmm10
	movhps	1 * SIZE(Y), %xmm10
	addq	INCY, Y
	mulpd	%xmm6,  %xmm12
	MOVLPS	0 * SIZE(X), %xmm6
	movhps	1 * SIZE(X), %xmm6
	addq	INCX, X
	addpd	%xmm12, %xmm1

	pshufd	$0x4e,  %xmm11, %xmm12
	mulpd	%xmm7,  %xmm11
	addpd	%xmm11, %xmm2
	MOVLPS	0 * SIZE(Y), %xmm11
	movhps	1 * SIZE(Y), %xmm11
	addq	INCY, Y
	mulpd	%xmm7,  %xmm12
	MOVLPS	0 * SIZE(X), %xmm7
	movhps	1 * SIZE(X), %xmm7
	addq	INCX, X
	addpd	%xmm12, %xmm3

	pshufd	$0x4e,  %xmm8, %xmm12
	mulpd	%xmm4,  %xmm8
	addpd	%xmm8,  %xmm0
	MOVLPS	0 * SIZE(Y), %xmm8
	movhps	1 * SIZE(Y), %xmm8
	addq	INCY, Y

	mulpd	%xmm4,  %xmm12
	MOVLPS	0 * SIZE(X), %xmm4
	movhps	1 * SIZE(X), %xmm4
	addq	INCX, X
	addpd	%xmm12, %xmm1

	pshufd	$0x4e,  %xmm9, %xmm12
	mulpd	%xmm5,  %xmm9
	addpd	%xmm9,  %xmm2
	MOVLPS	0 * SIZE(Y), %xmm9
	movhps	1 * SIZE(Y), %xmm9
	addq	INCY, Y

	mulpd	%xmm5,  %xmm12
	MOVLPS	0 * SIZE(X), %xmm5
	movhps	1 * SIZE(X), %xmm5
	addq	INCX, X
	addpd	%xmm12, %xmm3

	pshufd	$0x4e,  %xmm10, %xmm12
	mulpd	%xmm6,  %xmm10
	addpd	%xmm10, %xmm0
	MOVLPS	0 * SIZE(Y), %xmm10
	movhps	1 * SIZE(Y), %xmm10
	addq	INCY, Y
	mulpd	%xmm6,  %xmm12
	MOVLPS	0 * SIZE(X), %xmm6
	movhps	1 * SIZE(X), %xmm6
	addq	INCX, X
	addpd	%xmm12, %xmm1

	pshufd	$0x4e,  %xmm11, %xmm12
	mulpd	%xmm7,  %xmm11
	addpd	%xmm11, %xmm2
	MOVLPS	0 * SIZE(Y), %xmm11
	movhps	1 * SIZE(Y), %xmm11
	addq	INCY, Y
	mulpd	%xmm7,  %xmm12
	MOVLPS	0 * SIZE(X), %xmm7
	movhps	1 * SIZE(X), %xmm7
	addq	INCX, X
	addpd	%xmm12, %xmm3

	decq	%rax
	jg	.L53
	ALIGN_3

.L54:
	pshufd	$0x4e,  %xmm8, %xmm12
	mulpd	%xmm4,  %xmm8
	addpd	%xmm8,  %xmm0
	MOVLPS	0 * SIZE(Y), %xmm8
	movhps	1 * SIZE(Y), %xmm8
	addq	INCY, Y
	mulpd	%xmm4,  %xmm12
	MOVLPS	0 * SIZE(X), %xmm4
	movhps	1 * SIZE(X), %xmm4
	addq	INCX, X
	addpd	%xmm12, %xmm1

	pshufd	$0x4e,  %xmm9, %xmm12
	mulpd	%xmm5,  %xmm9
	addpd	%xmm9,  %xmm2
	MOVLPS	0 * SIZE(Y), %xmm9
	movhps	1 * SIZE(Y), %xmm9
	addq	INCY, Y
	mulpd	%xmm5,  %xmm12
	MOVLPS	0 * SIZE(X), %xmm5
	movhps	1 * SIZE(X), %xmm5
	addq	INCX, X
	addpd	%xmm12, %xmm3

	pshufd	$0x4e,  %xmm10, %xmm12
	mulpd	%xmm6,  %xmm10
	addpd	%xmm10, %xmm0
	MOVLPS	0 * SIZE(Y), %xmm10
	movhps	1 * SIZE(Y), %xmm10
	addq	INCY, Y
	mulpd	%xmm6,  %xmm12
	MOVLPS	0 * SIZE(X), %xmm6
	movhps	1 * SIZE(X), %xmm6
	addq	INCX, X
	addpd	%xmm12, %xmm1

	pshufd	$0x4e,  %xmm11, %xmm12
	mulpd	%xmm7,  %xmm11
	addpd	%xmm11, %xmm2
	MOVLPS	0 * SIZE(Y), %xmm11
	movhps	1 * SIZE(Y), %xmm11
	addq	INCY, Y
	mulpd	%xmm7,  %xmm12
	MOVLPS	0 * SIZE(X), %xmm7
	movhps	1 * SIZE(X), %xmm7
	addq	INCX, X
	addpd	%xmm12, %xmm3

	pshufd	$0x4e,  %xmm8, %xmm12
	mulpd	%xmm4,  %xmm8
	addpd	%xmm8,  %xmm0
	mulpd	%xmm4,  %xmm12
	addpd	%xmm12, %xmm1

	pshufd	$0x4e,  %xmm9, %xmm12
	mulpd	%xmm5,  %xmm9
	addpd	%xmm9,  %xmm2
	mulpd	%xmm5,  %xmm12
	addpd	%xmm12, %xmm3

	pshufd	$0x4e,  %xmm10, %xmm12
	mulpd	%xmm6,  %xmm10
	addpd	%xmm10, %xmm0
	mulpd	%xmm6,  %xmm12
	addpd	%xmm12, %xmm1

	pshufd	$0x4e,  %xmm11, %xmm12
	mulpd	%xmm7,  %xmm11
	addpd	%xmm11, %xmm2
	mulpd	%xmm7,  %xmm12
	addpd	%xmm12, %xmm3
	ALIGN_3

.L55:
	testq	$4, N
	jle	.L56

	MOVLPS	0 * SIZE(X), %xmm4
	movhps	1 * SIZE(X), %xmm4
	addq	INCX, X
	MOVLPS	0 * SIZE(Y), %xmm8
	movhps	1 * SIZE(Y), %xmm8
	addq	INCY, Y

	pshufd	$0x4e,  %xmm8, %xmm12
	mulpd	%xmm4,  %xmm8
	addpd	%xmm8,  %xmm0
	mulpd	%xmm4,  %xmm12
	addpd	%xmm12, %xmm1

	MOVLPS	0 * SIZE(X), %xmm5
	movhps	1 * SIZE(X), %xmm5
	addq	INCX, X
	MOVLPS	0 * SIZE(Y), %xmm9
	movhps	1 * SIZE(Y), %xmm9
	addq	INCY, Y

	pshufd	$0x4e,  %xmm9, %xmm12
	mulpd	%xmm5,  %xmm9
	addpd	%xmm9,  %xmm2
	mulpd	%xmm5,  %xmm12
	addpd	%xmm12, %xmm3

	MOVLPS	0 * SIZE(X), %xmm6
	movhps	1 * SIZE(X), %xmm6
	addq	INCX, X
	MOVLPS	0 * SIZE(Y), %xmm10
	movhps	1 * SIZE(Y), %xmm10
	addq	INCY, Y

	pshufd	$0x4e,  %xmm10, %xmm12
	mulpd	%xmm6,  %xmm10
	addpd	%xmm10, %xmm0
	mulpd	%xmm6,  %xmm12
	addpd	%xmm12, %xmm1

	MOVLPS	0 * SIZE(X), %xmm7
	movhps	1 * SIZE(X), %xmm7
	addq	INCX, X
	MOVLPS	0 * SIZE(Y), %xmm11
	movhps	1 * SIZE(Y), %xmm11
	addq	INCY, Y

	pshufd	$0x4e,  %xmm11, %xmm12
	mulpd	%xmm7,  %xmm11
	addpd	%xmm11, %xmm2
	mulpd	%xmm7,  %xmm12
	addpd	%xmm12, %xmm3
	ALIGN_3

.L56:
	testq	$2, N
	jle	.L57

	MOVLPS	0 * SIZE(X), %xmm4
	movhps	1 * SIZE(X), %xmm4
	addq	INCX, X
	MOVLPS	0 * SIZE(Y), %xmm8
	movhps	1 * SIZE(Y), %xmm8
	addq	INCY, Y

	pshufd	$0x4e,  %xmm8, %xmm12
	mulpd	%xmm4,  %xmm8
	addpd	%xmm8,  %xmm0
	mulpd	%xmm4,  %xmm12
	addpd	%xmm12, %xmm1

	MOVLPS	0 * SIZE(X), %xmm5
	movhps	1 * SIZE(X), %xmm5
	addq	INCX, X
	MOVLPS	0 * SIZE(Y), %xmm9
	movhps	1 * SIZE(Y), %xmm9
	addq	INCY, Y

	pshufd	$0x4e,  %xmm9, %xmm12
	mulpd	%xmm5,  %xmm9
	addpd	%xmm9,  %xmm2
	mulpd	%xmm5,  %xmm12
	addpd	%xmm12, %xmm3
	ALIGN_3

.L57:
	testq	$1, N
	jle	.L98

	MOVLPS	0 * SIZE(X), %xmm4
	movhps	1 * SIZE(X), %xmm4
	MOVLPS	0 * SIZE(Y), %xmm8
	movhps	1 * SIZE(Y), %xmm8

	pshufd	$0x4e,  %xmm8, %xmm12
	mulpd	%xmm4,  %xmm8
	addpd	%xmm8,  %xmm0
	mulpd	%xmm4,  %xmm12
	addpd	%xmm12, %xmm1
	ALIGN_3

.L98:
	addpd	%xmm2, %xmm0
	addpd	%xmm3, %xmm1

	pshufd	$0x4e, %xmm0, %xmm2
	pshufd	$0x4e, %xmm1, %xmm3

.L999:
#ifndef CONJ
	subsd	 %xmm2, %xmm0
	addsd	 %xmm3, %xmm1
#else
	addsd	 %xmm2, %xmm0
	subsd	 %xmm3, %xmm1
#endif

#ifdef WINDOWS_ABI
	movq	RESULT_ADDRESS,	%rax
	movsd	%xmm0,	(%rax)
	movsd	%xmm1,	8(%rax)
#endif

	RESTOREREGISTERS
	ret

	EPILOGUE
