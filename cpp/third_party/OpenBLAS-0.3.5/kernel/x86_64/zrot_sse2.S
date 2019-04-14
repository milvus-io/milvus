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

#define C	%xmm14
#define S	%xmm15

#include "l1param.h"

	PROLOGUE
	PROFCODE

#ifdef WINDOWS_ABI
	movq	40(%rsp), INCY
	movsd	48(%rsp), %xmm0
	movsd	56(%rsp), %xmm1
#endif

	SAVEREGISTERS

	salq	$ZBASE_SHIFT, INCX
	salq	$ZBASE_SHIFT, INCY

	pshufd	$0x44, %xmm0, C
	pshufd	$0x44, %xmm1, S

	cmpq	$0, N
	jle	.L999

	cmpq	$2 * SIZE, INCX
	jne	.L50
	cmpq	$2 * SIZE, INCY
	jne	.L50

.L10:
	testq	$SIZE, X
	jne	.L30

	testq	$SIZE, Y
	jne	.L20

	movq	N,  %rax
	sarq	$3, %rax
	jle	.L14

	movapd	0 * SIZE(Y), %xmm1
	movapd	2 * SIZE(Y), %xmm3
	movapd	4 * SIZE(Y), %xmm9
	movapd	6 * SIZE(Y), %xmm11

	movapd	0 * SIZE(X), %xmm0
	movapd	2 * SIZE(X), %xmm2
	movapd	4 * SIZE(X), %xmm8
	movapd	6 * SIZE(X), %xmm10

	decq	%rax
	jle	.L12
	ALIGN_3

.L11:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movapd	%xmm1, %xmm4
	mulpd	S, %xmm1
	movapd	%xmm3, %xmm6
	mulpd	S, %xmm3
	movapd	%xmm0, %xmm5
	mulpd	C, %xmm0
	movapd	%xmm2, %xmm7
	mulpd	C, %xmm2

	mulpd	C, %xmm4
	mulpd	S, %xmm5
	mulpd	C, %xmm6
	mulpd	S, %xmm7

	addpd	%xmm1, %xmm0
	movapd	 8 * SIZE(Y), %xmm1
	addpd	%xmm3, %xmm2
	movapd	10 * SIZE(Y), %xmm3
	subpd	%xmm5, %xmm4
	subpd	%xmm7, %xmm6

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	movapd	%xmm0, 0 * SIZE(X)
	movapd	 8 * SIZE(X), %xmm0
	movapd	%xmm2, 2 * SIZE(X)
	movapd	10 * SIZE(X), %xmm2
	movapd	%xmm4, 0 * SIZE(Y)
	movapd	%xmm6, 2 * SIZE(Y)

	movapd	%xmm9, %xmm4
	mulpd	S, %xmm9
	movapd	%xmm8, %xmm5
	mulpd	C, %xmm8
	movapd	%xmm11, %xmm6
	mulpd	S, %xmm11
	movapd	%xmm10, %xmm7
	mulpd	C, %xmm10

	mulpd	C, %xmm4
	mulpd	S, %xmm5
	mulpd	C, %xmm6
	mulpd	S, %xmm7

	addpd	%xmm9,  %xmm8
	movapd	12 * SIZE(Y), %xmm9
	addpd	%xmm11, %xmm10
	movapd	14 * SIZE(Y), %xmm11
	subpd	%xmm5,  %xmm4
	subpd	%xmm7,  %xmm6

	movapd	%xmm8, 4 * SIZE(X)
	movapd	12 * SIZE(X), %xmm8
	movapd	%xmm10,6 * SIZE(X)
	movapd	14 * SIZE(X), %xmm10
	movapd	%xmm4, 4 * SIZE(Y)
	movapd	%xmm6, 6 * SIZE(Y)

#if defined(PREFETCHW) && !defined(FETCH128)
	PREFETCHW (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	movapd	%xmm1, %xmm4
	mulpd	S, %xmm1
	movapd	%xmm3, %xmm6
	mulpd	S, %xmm3
	movapd	%xmm0, %xmm5
	mulpd	C, %xmm0
	movapd	%xmm2, %xmm7
	mulpd	C, %xmm2

	mulpd	C, %xmm4
	mulpd	S, %xmm5
	mulpd	C, %xmm6
	mulpd	S, %xmm7

	addpd	%xmm1, %xmm0
	movapd	16 * SIZE(Y), %xmm1
	addpd	%xmm3, %xmm2
	movapd	18 * SIZE(Y), %xmm3
	subpd	%xmm5, %xmm4
	subpd	%xmm7, %xmm6

	movapd	%xmm0,  8 * SIZE(X)
	movapd	16 * SIZE(X), %xmm0
	movapd	%xmm2, 10 * SIZE(X)
	movapd	18 * SIZE(X), %xmm2
	movapd	%xmm4,  8 * SIZE(Y)
	movapd	%xmm6, 10 * SIZE(Y)

#if defined(PREFETCHW) && !defined(FETCH128)
	PREFETCHW (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	movapd	%xmm9, %xmm4
	mulpd	S, %xmm9
	movapd	%xmm8, %xmm5
	mulpd	C, %xmm8
	movapd	%xmm11, %xmm6
	mulpd	S, %xmm11
	movapd	%xmm10, %xmm7
	mulpd	C, %xmm10

	mulpd	C, %xmm4
	mulpd	S, %xmm5
	mulpd	C, %xmm6
	mulpd	S, %xmm7

	addpd	%xmm9,  %xmm8
	movapd	20 * SIZE(Y), %xmm9
	addpd	%xmm11, %xmm10
	movapd	22 * SIZE(Y), %xmm11
	subpd	%xmm5,  %xmm4
	subpd	%xmm7,  %xmm6

	movapd	%xmm8,  12 * SIZE(X)
	movapd	20 * SIZE(X), %xmm8
	movapd	%xmm10, 14 * SIZE(X)
	movapd	22 * SIZE(X), %xmm10
	movapd	%xmm4,  12 * SIZE(Y)
	movapd	%xmm6,  14 * SIZE(Y)

	addq	$16 * SIZE, X
	addq	$16 * SIZE, Y

	decq	%rax
	jg	.L11
	ALIGN_3

.L12:
	movapd	%xmm1, %xmm4
	mulpd	S, %xmm1
	movapd	%xmm3, %xmm6
	mulpd	S, %xmm3
	movapd	%xmm0, %xmm5
	mulpd	C, %xmm0
	movapd	%xmm2, %xmm7
	mulpd	C, %xmm2

	mulpd	C, %xmm4
	mulpd	S, %xmm5
	mulpd	C, %xmm6
	mulpd	S, %xmm7

	addpd	%xmm1, %xmm0
	movapd	 8 * SIZE(Y), %xmm1
	addpd	%xmm3, %xmm2
	movapd	10 * SIZE(Y), %xmm3
	subpd	%xmm5, %xmm4
	subpd	%xmm7, %xmm6

	movapd	%xmm0, 0 * SIZE(X)
	movapd	 8 * SIZE(X), %xmm0
	movapd	%xmm2, 2 * SIZE(X)
	movapd	10 * SIZE(X), %xmm2

	movapd	%xmm4, 0 * SIZE(Y)
	movapd	%xmm6, 2 * SIZE(Y)

	movapd	%xmm9, %xmm4
	mulpd	S, %xmm9
	movapd	%xmm8, %xmm5
	mulpd	C, %xmm8
	movapd	%xmm11, %xmm6
	mulpd	S, %xmm11
	movapd	%xmm10, %xmm7
	mulpd	C, %xmm10

	mulpd	C, %xmm4
	mulpd	S, %xmm5
	mulpd	C, %xmm6
	mulpd	S, %xmm7

	addpd	%xmm9,  %xmm8
	movapd	12 * SIZE(Y), %xmm9
	addpd	%xmm11, %xmm10
	movapd	14 * SIZE(Y), %xmm11
	subpd	%xmm5,  %xmm4
	subpd	%xmm7,  %xmm6

	movapd	%xmm8, 4 * SIZE(X)
	movapd	12 * SIZE(X), %xmm8
	movapd	%xmm10,6 * SIZE(X)
	movapd	14 * SIZE(X), %xmm10
	movapd	%xmm4, 4 * SIZE(Y)
	movapd	%xmm6, 6 * SIZE(Y)

	movapd	%xmm1, %xmm4
	mulpd	S, %xmm1
	movapd	%xmm3, %xmm6
	mulpd	S, %xmm3
	movapd	%xmm0, %xmm5
	mulpd	C, %xmm0
	movapd	%xmm2, %xmm7
	mulpd	C, %xmm2

	mulpd	C, %xmm4
	mulpd	S, %xmm5
	mulpd	C, %xmm6
	mulpd	S, %xmm7

	addpd	%xmm1, %xmm0
	addpd	%xmm3, %xmm2
	subpd	%xmm5, %xmm4
	subpd	%xmm7, %xmm6

	movapd	%xmm0,  8 * SIZE(X)
	movapd	%xmm2, 10 * SIZE(X)
	movapd	%xmm4,  8 * SIZE(Y)
	movapd	%xmm6, 10 * SIZE(Y)

	movapd	%xmm9, %xmm4
	mulpd	S, %xmm9
	movapd	%xmm8, %xmm5
	mulpd	C, %xmm8
	movapd	%xmm11, %xmm6
	mulpd	S, %xmm11
	movapd	%xmm10, %xmm7
	mulpd	C, %xmm10

	mulpd	C, %xmm4
	mulpd	S, %xmm5
	mulpd	C, %xmm6
	mulpd	S, %xmm7

	addpd	%xmm9,  %xmm8
	addpd	%xmm11, %xmm10
	subpd	%xmm5,  %xmm4
	subpd	%xmm7,  %xmm6

	movapd	%xmm8,  12 * SIZE(X)
	movapd	%xmm10, 14 * SIZE(X)
	movapd	%xmm4,  12 * SIZE(Y)
	movapd	%xmm6,  14 * SIZE(Y)

	addq	$16 * SIZE, X
	addq	$16 * SIZE, Y
	ALIGN_3

.L14:
	testq	$7, N
	jle	.L999

	testq	$4, N
	jle	.L15

	movapd	0 * SIZE(Y), %xmm1
	movapd	0 * SIZE(X), %xmm0
	movapd	2 * SIZE(Y), %xmm3
	movapd	2 * SIZE(X), %xmm2

	movapd	%xmm1, %xmm4
	movapd	%xmm0, %xmm5
	movapd	%xmm3, %xmm6
	movapd	%xmm2, %xmm7

	mulpd	C, %xmm0
	mulpd	S, %xmm1
	mulpd	C, %xmm2
	mulpd	S, %xmm3

	mulpd	C, %xmm4
	mulpd	S, %xmm5
	mulpd	C, %xmm6
	mulpd	S, %xmm7

	addpd	%xmm1, %xmm0
	addpd	%xmm3, %xmm2
	subpd	%xmm5, %xmm4
	subpd	%xmm7, %xmm6

	movapd	%xmm0, 0 * SIZE(X)
	movapd	%xmm2, 2 * SIZE(X)
	movapd	%xmm4, 0 * SIZE(Y)
	movapd	%xmm6, 2 * SIZE(Y)

	movapd	4 * SIZE(Y), %xmm1
	movapd	4 * SIZE(X), %xmm0
	movapd	6 * SIZE(Y), %xmm3
	movapd	6 * SIZE(X), %xmm2

	movapd	%xmm1, %xmm4
	movapd	%xmm0, %xmm5
	movapd	%xmm3, %xmm6
	movapd	%xmm2, %xmm7

	mulpd	C, %xmm0
	mulpd	S, %xmm1
	mulpd	C, %xmm2
	mulpd	S, %xmm3

	mulpd	C, %xmm4
	mulpd	S, %xmm5
	mulpd	C, %xmm6
	mulpd	S, %xmm7

	addpd	%xmm1, %xmm0
	addpd	%xmm3, %xmm2
	subpd	%xmm5, %xmm4
	subpd	%xmm7, %xmm6

	movapd	%xmm0, 4 * SIZE(X)
	movapd	%xmm2, 6 * SIZE(X)
	movapd	%xmm4, 4 * SIZE(Y)
	movapd	%xmm6, 6 * SIZE(Y)

	addq	$8 * SIZE, X
	addq	$8 * SIZE, Y
	ALIGN_3

.L15:
	testq	$2, N
	jle	.L16

	movapd	0 * SIZE(Y), %xmm1
	movapd	0 * SIZE(X), %xmm0
	movapd	2 * SIZE(Y), %xmm3
	movapd	2 * SIZE(X), %xmm2

	movapd	%xmm1, %xmm4
	movapd	%xmm0, %xmm5
	movapd	%xmm3, %xmm6
	movapd	%xmm2, %xmm7

	mulpd	C, %xmm0
	mulpd	S, %xmm1
	mulpd	C, %xmm2
	mulpd	S, %xmm3

	mulpd	C, %xmm4
	mulpd	S, %xmm5
	mulpd	C, %xmm6
	mulpd	S, %xmm7

	addpd	%xmm1, %xmm0
	addpd	%xmm3, %xmm2
	subpd	%xmm5, %xmm4
	subpd	%xmm7, %xmm6

	movapd	%xmm0, 0 * SIZE(X)
	movapd	%xmm2, 2 * SIZE(X)
	movapd	%xmm4, 0 * SIZE(Y)
	movapd	%xmm6, 2 * SIZE(Y)

	addq	$4 * SIZE, X
	addq	$4 * SIZE, Y
	ALIGN_3

.L16:
	testq	$1, N
	jle	.L999

	movapd	0 * SIZE(Y), %xmm1
	movapd	0 * SIZE(X), %xmm0

	movapd	%xmm1, %xmm2
	movapd	%xmm0, %xmm3

	mulpd	C, %xmm0
	mulpd	S, %xmm1

	mulpd	C, %xmm2
	mulpd	S, %xmm3

	addpd	%xmm1, %xmm0
	subpd	%xmm3, %xmm2

	movapd	%xmm0, 0 * SIZE(X)
	movapd	%xmm2, 0 * SIZE(Y)
	jmp	.L999
	ALIGN_3

.L20:
	movapd	-1 * SIZE(Y), %xmm1

	movq	N,  %rax
	sarq	$3, %rax
	jle	.L24
	ALIGN_3

.L21:
#if defined(PREFETCHW)
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movapd	1 * SIZE(Y), %xmm3
	movapd	3 * SIZE(Y), %xmm8
	movapd	0 * SIZE(X), %xmm0
	movapd	2 * SIZE(X), %xmm2

	SHUFPD_1 %xmm3, %xmm1
	SHUFPD_1 %xmm8, %xmm3

	movapd	%xmm1, %xmm4
	movapd	%xmm0, %xmm5
	movapd	%xmm3, %xmm6
	movapd	%xmm2, %xmm7

	mulpd	C, %xmm0
	mulpd	S, %xmm1
	mulpd	C, %xmm2
	mulpd	S, %xmm3

	mulpd	C, %xmm4
	mulpd	S, %xmm5
	mulpd	C, %xmm6
	mulpd	S, %xmm7

	addpd	%xmm1, %xmm0
	addpd	%xmm3, %xmm2
	subpd	%xmm5, %xmm4
	subpd	%xmm7, %xmm6

	movapd	%xmm0, 0 * SIZE(X)
	movapd	%xmm2, 2 * SIZE(X)

	movlps	%xmm4, 0 * SIZE(Y)
	movhps	%xmm4, 1 * SIZE(Y)
	movlps	%xmm6, 2 * SIZE(Y)
	movhps	%xmm6, 3 * SIZE(Y)

#if defined(PREFETCHW)
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	movapd	5 * SIZE(Y), %xmm9
	movapd	7 * SIZE(Y), %xmm1
	movapd	4 * SIZE(X), %xmm0
	movapd	6 * SIZE(X), %xmm2

	SHUFPD_1 %xmm9, %xmm8
	SHUFPD_1 %xmm1, %xmm9

	movapd	%xmm8, %xmm4
	movapd	%xmm0, %xmm5
	movapd	%xmm9, %xmm6
	movapd	%xmm2, %xmm7

	mulpd	C, %xmm0
	mulpd	S, %xmm8
	mulpd	C, %xmm2
	mulpd	S, %xmm9

	mulpd	C, %xmm4
	mulpd	S, %xmm5
	mulpd	C, %xmm6
	mulpd	S, %xmm7

	addpd	%xmm8, %xmm0
	addpd	%xmm9, %xmm2
	subpd	%xmm5, %xmm4
	subpd	%xmm7, %xmm6

	movapd	%xmm0, 4 * SIZE(X)
	movapd	%xmm2, 6 * SIZE(X)
	movlps	%xmm4, 4 * SIZE(Y)
	movhps	%xmm4, 5 * SIZE(Y)
	movlps	%xmm6, 6 * SIZE(Y)
	movhps	%xmm6, 7 * SIZE(Y)

#if defined(PREFETCHW) && !defined(FETCH128)
	PREFETCHW (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	movapd	 9 * SIZE(Y), %xmm3
	movapd	11 * SIZE(Y), %xmm8
	movapd	 8 * SIZE(X), %xmm0
	movapd	10 * SIZE(X), %xmm2

	SHUFPD_1 %xmm3, %xmm1
	SHUFPD_1 %xmm8, %xmm3

	movapd	%xmm1, %xmm4
	movapd	%xmm0, %xmm5
	movapd	%xmm3, %xmm6
	movapd	%xmm2, %xmm7

	mulpd	C, %xmm0
	mulpd	S, %xmm1
	mulpd	C, %xmm2
	mulpd	S, %xmm3

	mulpd	C, %xmm4
	mulpd	S, %xmm5
	mulpd	C, %xmm6
	mulpd	S, %xmm7

	addpd	%xmm1, %xmm0
	addpd	%xmm3, %xmm2
	subpd	%xmm5, %xmm4
	subpd	%xmm7, %xmm6

	movapd	%xmm0,  8 * SIZE(X)
	movapd	%xmm2, 10 * SIZE(X)
	movlps	%xmm4,  8 * SIZE(Y)
	movhps	%xmm4,  9 * SIZE(Y)
	movlps	%xmm6, 10 * SIZE(Y)
	movhps	%xmm6, 11 * SIZE(Y)

#if defined(PREFETCHW) && !defined(FETCH128)
	PREFETCHW (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	movapd	13 * SIZE(Y), %xmm9
	movapd	15 * SIZE(Y), %xmm1
	movapd	12 * SIZE(X), %xmm0
	movapd	14 * SIZE(X), %xmm2

	SHUFPD_1 %xmm9, %xmm8
	SHUFPD_1 %xmm1, %xmm9

	movapd	%xmm8, %xmm4
	movapd	%xmm0, %xmm5
	movapd	%xmm9, %xmm6
	movapd	%xmm2, %xmm7

	mulpd	C, %xmm0
	mulpd	S, %xmm8
	mulpd	C, %xmm2
	mulpd	S, %xmm9

	mulpd	C, %xmm4
	mulpd	S, %xmm5
	mulpd	C, %xmm6
	mulpd	S, %xmm7

	addpd	%xmm8, %xmm0
	addpd	%xmm9, %xmm2
	subpd	%xmm5, %xmm4
	subpd	%xmm7, %xmm6

	movapd	%xmm0, 12 * SIZE(X)
	movapd	%xmm2, 14 * SIZE(X)
	movlps	%xmm4, 12 * SIZE(Y)
	movhps	%xmm4, 13 * SIZE(Y)
	movlps	%xmm6, 14 * SIZE(Y)
	movhps	%xmm6, 15 * SIZE(Y)

	addq	$16 * SIZE, X
	addq	$16 * SIZE, Y
	decq	%rax
	jg	.L21
	ALIGN_3

.L24:
	testq	$7, N
	jle	.L999

	testq	$4, N
	jle	.L25

	movapd	1 * SIZE(Y), %xmm3
	movapd	3 * SIZE(Y), %xmm8
	movapd	0 * SIZE(X), %xmm0
	movapd	2 * SIZE(X), %xmm2

	SHUFPD_1 %xmm3, %xmm1
	SHUFPD_1 %xmm8, %xmm3

	movapd	%xmm1, %xmm4
	movapd	%xmm0, %xmm5
	movapd	%xmm3, %xmm6
	movapd	%xmm2, %xmm7

	mulpd	C, %xmm0
	mulpd	S, %xmm1
	mulpd	C, %xmm2
	mulpd	S, %xmm3

	mulpd	C, %xmm4
	mulpd	S, %xmm5
	mulpd	C, %xmm6
	mulpd	S, %xmm7

	addpd	%xmm1, %xmm0
	addpd	%xmm3, %xmm2
	subpd	%xmm5, %xmm4
	subpd	%xmm7, %xmm6

	movapd	%xmm0, 0 * SIZE(X)
	movapd	%xmm2, 2 * SIZE(X)
	movlps	%xmm4, 0 * SIZE(Y)
	movhps	%xmm4, 1 * SIZE(Y)
	movlps	%xmm6, 2 * SIZE(Y)
	movhps	%xmm6, 3 * SIZE(Y)

	movapd	5 * SIZE(Y), %xmm9
	movapd	7 * SIZE(Y), %xmm1
	movapd	4 * SIZE(X), %xmm0
	movapd	6 * SIZE(X), %xmm2

	SHUFPD_1 %xmm9, %xmm8
	SHUFPD_1 %xmm1, %xmm9

	movapd	%xmm8, %xmm4
	movapd	%xmm0, %xmm5
	movapd	%xmm9, %xmm6
	movapd	%xmm2, %xmm7

	mulpd	C, %xmm0
	mulpd	S, %xmm8
	mulpd	C, %xmm2
	mulpd	S, %xmm9

	mulpd	C, %xmm4
	mulpd	S, %xmm5
	mulpd	C, %xmm6
	mulpd	S, %xmm7

	addpd	%xmm8, %xmm0
	addpd	%xmm9, %xmm2
	subpd	%xmm5, %xmm4
	subpd	%xmm7, %xmm6

	movapd	%xmm0, 4 * SIZE(X)
	movapd	%xmm2, 6 * SIZE(X)
	movlps	%xmm4, 4 * SIZE(Y)
	movhps	%xmm4, 5 * SIZE(Y)
	movlps	%xmm6, 6 * SIZE(Y)
	movhps	%xmm6, 7 * SIZE(Y)

	addq	$8 * SIZE, X
	addq	$8 * SIZE, Y
	ALIGN_3

.L25:
	testq	$2, N
	jle	.L26

	movapd	1 * SIZE(Y), %xmm3
	movapd	3 * SIZE(Y), %xmm8
	movapd	0 * SIZE(X), %xmm0
	movapd	2 * SIZE(X), %xmm2

	SHUFPD_1 %xmm3, %xmm1
	SHUFPD_1 %xmm8, %xmm3

	movapd	%xmm1, %xmm4
	movapd	%xmm0, %xmm5
	movapd	%xmm3, %xmm6
	movapd	%xmm2, %xmm7

	mulpd	C, %xmm0
	mulpd	S, %xmm1
	mulpd	C, %xmm2
	mulpd	S, %xmm3

	mulpd	C, %xmm4
	mulpd	S, %xmm5
	mulpd	C, %xmm6
	mulpd	S, %xmm7

	addpd	%xmm1, %xmm0
	addpd	%xmm3, %xmm2
	subpd	%xmm5, %xmm4
	subpd	%xmm7, %xmm6

	movapd	%xmm0, 0 * SIZE(X)
	movapd	%xmm2, 2 * SIZE(X)
	movlps	%xmm4, 0 * SIZE(Y)
	movhps	%xmm4, 1 * SIZE(Y)
	movlps	%xmm6, 2 * SIZE(Y)
	movhps	%xmm6, 3 * SIZE(Y)
	movapd	%xmm8, %xmm1

	addq	$4 * SIZE, X
	addq	$4 * SIZE, Y
	ALIGN_3

.L26:
	testq	$1, N
	jle	.L999

	movapd	1 * SIZE(Y), %xmm4
	movapd	0 * SIZE(X), %xmm0

	SHUFPD_1 %xmm4, %xmm1
	movapd	%xmm1, %xmm2
	movapd	%xmm0, %xmm3

	mulpd	C, %xmm0
	mulpd	S, %xmm1

	mulpd	C, %xmm2
	mulpd	S, %xmm3

	addpd	%xmm1, %xmm0
	subpd	%xmm3, %xmm2

	movapd	%xmm0, 0 * SIZE(X)
	movlps	%xmm2, 0 * SIZE(Y)
	movhps	%xmm2, 1 * SIZE(Y)
	jmp	.L999
	ALIGN_3

.L30:
	testq	$SIZE, Y
	jne	.L40

	movapd	-1 * SIZE(X), %xmm0

	movq	N,  %rax
	sarq	$3, %rax
	jle	.L34
	ALIGN_3

.L31:
#if defined(PREFETCHW)
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movapd	1 * SIZE(X), %xmm2
	movapd	3 * SIZE(X), %xmm8
	movapd	0 * SIZE(Y), %xmm1
	movapd	2 * SIZE(Y), %xmm3

	SHUFPD_1 %xmm2, %xmm0
	SHUFPD_1 %xmm8, %xmm2

	movapd	%xmm1, %xmm4
	movapd	%xmm0, %xmm5
	movapd	%xmm3, %xmm6
	movapd	%xmm2, %xmm7

	mulpd	C, %xmm0
	mulpd	S, %xmm1
	mulpd	C, %xmm2
	mulpd	S, %xmm3

	mulpd	C, %xmm4
	mulpd	S, %xmm5
	mulpd	C, %xmm6
	mulpd	S, %xmm7

	addpd	%xmm1, %xmm0
	addpd	%xmm3, %xmm2
	subpd	%xmm5, %xmm4
	subpd	%xmm7, %xmm6

	movlps	%xmm0, 0 * SIZE(X)
	movhps	%xmm0, 1 * SIZE(X)
	movlps	%xmm2, 2 * SIZE(X)
	movhps	%xmm2, 3 * SIZE(X)
	movapd	%xmm4, 0 * SIZE(Y)
	movapd	%xmm6, 2 * SIZE(Y)

#if defined(PREFETCHW)
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	movapd	5 * SIZE(X), %xmm2
	movapd	7 * SIZE(X), %xmm0
	movapd	4 * SIZE(Y), %xmm1
	movapd	6 * SIZE(Y), %xmm3

	SHUFPD_1 %xmm2, %xmm8
	SHUFPD_1 %xmm0, %xmm2

	movapd	%xmm1, %xmm4
	movapd	%xmm8, %xmm5
	movapd	%xmm3, %xmm6
	movapd	%xmm2, %xmm7

	mulpd	C, %xmm8
	mulpd	S, %xmm1
	mulpd	C, %xmm2
	mulpd	S, %xmm3

	mulpd	C, %xmm4
	mulpd	S, %xmm5
	mulpd	C, %xmm6
	mulpd	S, %xmm7

	addpd	%xmm1, %xmm8
	addpd	%xmm3, %xmm2
	subpd	%xmm5, %xmm4
	subpd	%xmm7, %xmm6

	movlps	%xmm8, 4 * SIZE(X)
	movhps	%xmm8, 5 * SIZE(X)
	movlps	%xmm2, 6 * SIZE(X)
	movhps	%xmm2, 7 * SIZE(X)
	movapd	%xmm4, 4 * SIZE(Y)
	movapd	%xmm6, 6 * SIZE(Y)

#if defined(PREFETCHW) && !defined(FETCH128)
	PREFETCHW (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	movapd	 9 * SIZE(X), %xmm2
	movapd	11 * SIZE(X), %xmm8
	movapd	 8 * SIZE(Y), %xmm1
	movapd	10 * SIZE(Y), %xmm3

	SHUFPD_1 %xmm2, %xmm0
	SHUFPD_1 %xmm8, %xmm2

	movapd	%xmm1, %xmm4
	movapd	%xmm0, %xmm5
	movapd	%xmm3, %xmm6
	movapd	%xmm2, %xmm7

	mulpd	C, %xmm0
	mulpd	S, %xmm1
	mulpd	C, %xmm2
	mulpd	S, %xmm3

	mulpd	C, %xmm4
	mulpd	S, %xmm5
	mulpd	C, %xmm6
	mulpd	S, %xmm7

	addpd	%xmm1, %xmm0
	addpd	%xmm3, %xmm2
	subpd	%xmm5, %xmm4
	subpd	%xmm7, %xmm6

	movlps	%xmm0,  8 * SIZE(X)
	movhps	%xmm0,  9 * SIZE(X)
	movlps	%xmm2, 10 * SIZE(X)
	movhps	%xmm2, 11 * SIZE(X)
	movapd	%xmm4,  8 * SIZE(Y)
	movapd	%xmm6, 10 * SIZE(Y)

#if defined(PREFETCHW) && !defined(FETCH128)
	PREFETCHW (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	movapd	13 * SIZE(X), %xmm2
	movapd	15 * SIZE(X), %xmm0
	movapd	12 * SIZE(Y), %xmm1
	movapd	14 * SIZE(Y), %xmm3

	SHUFPD_1 %xmm2, %xmm8
	SHUFPD_1 %xmm0, %xmm2

	movapd	%xmm1, %xmm4
	movapd	%xmm8, %xmm5
	movapd	%xmm3, %xmm6
	movapd	%xmm2, %xmm7

	mulpd	C, %xmm8
	mulpd	S, %xmm1
	mulpd	C, %xmm2
	mulpd	S, %xmm3

	mulpd	C, %xmm4
	mulpd	S, %xmm5
	mulpd	C, %xmm6
	mulpd	S, %xmm7

	addpd	%xmm1, %xmm8
	addpd	%xmm3, %xmm2
	subpd	%xmm5, %xmm4
	subpd	%xmm7, %xmm6

	movlps	%xmm8, 12 * SIZE(X)
	movhps	%xmm8, 13 * SIZE(X)
	movlps	%xmm2, 14 * SIZE(X)
	movhps	%xmm2, 15 * SIZE(X)
	movapd	%xmm4, 12 * SIZE(Y)
	movapd	%xmm6, 14 * SIZE(Y)

	addq	$16 * SIZE, Y
	addq	$16 * SIZE, X
	decq	%rax
	jg	.L31
	ALIGN_3

.L34:
	testq	$7, N
	jle	.L999

	testq	$4, N
	jle	.L35

	movapd	1 * SIZE(X), %xmm2
	movapd	3 * SIZE(X), %xmm8
	movapd	0 * SIZE(Y), %xmm1
	movapd	2 * SIZE(Y), %xmm3

	SHUFPD_1 %xmm2, %xmm0
	SHUFPD_1 %xmm8, %xmm2

	movapd	%xmm1, %xmm4
	movapd	%xmm0, %xmm5
	movapd	%xmm3, %xmm6
	movapd	%xmm2, %xmm7

	mulpd	C, %xmm0
	mulpd	S, %xmm1
	mulpd	C, %xmm2
	mulpd	S, %xmm3

	mulpd	C, %xmm4
	mulpd	S, %xmm5
	mulpd	C, %xmm6
	mulpd	S, %xmm7

	addpd	%xmm1, %xmm0
	addpd	%xmm3, %xmm2
	subpd	%xmm5, %xmm4
	subpd	%xmm7, %xmm6

	movlps	%xmm0, 0 * SIZE(X)
	movhps	%xmm0, 1 * SIZE(X)
	movlps	%xmm2, 2 * SIZE(X)
	movhps	%xmm2, 3 * SIZE(X)
	movapd	%xmm4, 0 * SIZE(Y)
	movapd	%xmm6, 2 * SIZE(Y)

	movapd	5 * SIZE(X), %xmm2
	movapd	7 * SIZE(X), %xmm0
	movapd	4 * SIZE(Y), %xmm1
	movapd	6 * SIZE(Y), %xmm3

	SHUFPD_1 %xmm2, %xmm8
	SHUFPD_1 %xmm0, %xmm2

	movapd	%xmm1, %xmm4
	movapd	%xmm8, %xmm5
	movapd	%xmm3, %xmm6
	movapd	%xmm2, %xmm7

	mulpd	C, %xmm8
	mulpd	S, %xmm1
	mulpd	C, %xmm2
	mulpd	S, %xmm3

	mulpd	C, %xmm4
	mulpd	S, %xmm5
	mulpd	C, %xmm6
	mulpd	S, %xmm7

	addpd	%xmm1, %xmm8
	addpd	%xmm3, %xmm2
	subpd	%xmm5, %xmm4
	subpd	%xmm7, %xmm6

	movlps	%xmm8, 4 * SIZE(X)
	movhps	%xmm8, 5 * SIZE(X)
	movlps	%xmm2, 6 * SIZE(X)
	movhps	%xmm2, 7 * SIZE(X)
	movapd	%xmm4, 4 * SIZE(Y)
	movapd	%xmm6, 6 * SIZE(Y)

	addq	$8 * SIZE, Y
	addq	$8 * SIZE, X
	ALIGN_3

.L35:
	testq	$2, N
	jle	.L36

	movapd	1 * SIZE(X), %xmm2
	movapd	3 * SIZE(X), %xmm8

	movapd	0 * SIZE(Y), %xmm1
	movapd	2 * SIZE(Y), %xmm3

	SHUFPD_1 %xmm2, %xmm0
	SHUFPD_1 %xmm8, %xmm2

	movapd	%xmm1, %xmm4
	movapd	%xmm0, %xmm5
	movapd	%xmm3, %xmm6
	movapd	%xmm2, %xmm7

	mulpd	C, %xmm0
	mulpd	S, %xmm1
	mulpd	C, %xmm2
	mulpd	S, %xmm3

	mulpd	C, %xmm4
	mulpd	S, %xmm5
	mulpd	C, %xmm6
	mulpd	S, %xmm7

	addpd	%xmm1, %xmm0
	addpd	%xmm3, %xmm2
	subpd	%xmm5, %xmm4
	subpd	%xmm7, %xmm6

	movlps	%xmm0, 0 * SIZE(X)
	movhps	%xmm0, 1 * SIZE(X)
	movlps	%xmm2, 2 * SIZE(X)
	movhps	%xmm2, 3 * SIZE(X)
	movapd	%xmm4, 0 * SIZE(Y)
	movapd	%xmm6, 2 * SIZE(Y)
	movapd	%xmm8, %xmm0

	addq	$4 * SIZE, Y
	addq	$4 * SIZE, X
	ALIGN_3

.L36:
	testq	$1, N
	jle	.L999

	movapd	 1 * SIZE(X), %xmm4
	movapd	 0 * SIZE(Y), %xmm1

	SHUFPD_1 %xmm4, %xmm0
	movapd	%xmm1, %xmm2
	movapd	%xmm0, %xmm3

	mulpd	C, %xmm0
	mulpd	S, %xmm1

	mulpd	C, %xmm2
	mulpd	S, %xmm3

	addpd	%xmm1, %xmm0
	subpd	%xmm3, %xmm2

	movlps	%xmm0, 0 * SIZE(X)
	movhps	%xmm0, 1 * SIZE(X)
	movapd	%xmm2, 0 * SIZE(Y)
	jmp	.L999
	ALIGN_3

.L40:
	movsd	0 * SIZE(Y), %xmm1
	movsd	0 * SIZE(X), %xmm0

	movapd	%xmm1, %xmm2
	movapd	%xmm0, %xmm3

	mulsd	C, %xmm0
	mulsd	S, %xmm1

	mulsd	C, %xmm2
	mulsd	S, %xmm3

	addsd	%xmm1, %xmm0
	subsd	%xmm3, %xmm2

	movsd	%xmm0, 0 * SIZE(X)
	movsd	%xmm2, 0 * SIZE(Y)
	addq	$1 * SIZE, Y
	addq	$1 * SIZE, X

	decq	N
	jle	.L47

	movq	N,  %rax
	sarq	$3, %rax
	jle	.L44

	movapd	0 * SIZE(Y), %xmm1
	movapd	2 * SIZE(Y), %xmm3
	movapd	4 * SIZE(Y), %xmm9
	movapd	6 * SIZE(Y), %xmm11

	movapd	0 * SIZE(X), %xmm0
	movapd	2 * SIZE(X), %xmm2
	movapd	4 * SIZE(X), %xmm8
	movapd	6 * SIZE(X), %xmm10

	decq	%rax
	jle	.L42
	ALIGN_3

.L41:
#if defined(PREFETCHW)
	PREFETCHW (PREFETCHSIZE +   0) - PREOFFSET(X)
#endif

	movapd	%xmm1, %xmm4
	mulpd	S, %xmm1
	movapd	%xmm3, %xmm6
	mulpd	S, %xmm3
	movapd	%xmm0, %xmm5
	mulpd	C, %xmm0
	movapd	%xmm2, %xmm7
	mulpd	C, %xmm2

	mulpd	C, %xmm4
	mulpd	S, %xmm5
	mulpd	C, %xmm6
	mulpd	S, %xmm7

	addpd	%xmm1, %xmm0
	movapd	 8 * SIZE(Y), %xmm1
	addpd	%xmm3, %xmm2
	movapd	10 * SIZE(Y), %xmm3
	subpd	%xmm5, %xmm4
	subpd	%xmm7, %xmm6

#if defined(PREFETCHW)
	PREFETCHW (PREFETCHSIZE +   0) - PREOFFSET(Y)
#endif

	movapd	%xmm0, 0 * SIZE(X)
	movapd	 8 * SIZE(X), %xmm0
	movapd	%xmm2, 2 * SIZE(X)
	movapd	10 * SIZE(X), %xmm2
	movapd	%xmm4, 0 * SIZE(Y)
	movapd	%xmm6, 2 * SIZE(Y)

	movapd	%xmm9, %xmm4
	mulpd	S, %xmm9
	movapd	%xmm8, %xmm5
	mulpd	C, %xmm8
	movapd	%xmm11, %xmm6
	mulpd	S, %xmm11
	movapd	%xmm10, %xmm7
	mulpd	C, %xmm10

	mulpd	C, %xmm4
	mulpd	S, %xmm5
	mulpd	C, %xmm6
	mulpd	S, %xmm7

	addpd	%xmm9,  %xmm8
	movapd	12 * SIZE(Y), %xmm9
	addpd	%xmm11, %xmm10
	movapd	14 * SIZE(Y), %xmm11
	subpd	%xmm5,  %xmm4
	subpd	%xmm7,  %xmm6

	movapd	%xmm8, 4 * SIZE(X)
	movapd	12 * SIZE(X), %xmm8
	movapd	%xmm10,6 * SIZE(X)
	movapd	14 * SIZE(X), %xmm10
	movapd	%xmm4, 4 * SIZE(Y)
	movapd	%xmm6, 6 * SIZE(Y)

#if defined(PREFETCHW) && !defined(FETCH128)
	PREFETCHW (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	movapd	%xmm1, %xmm4
	mulpd	S, %xmm1
	movapd	%xmm3, %xmm6
	mulpd	S, %xmm3
	movapd	%xmm0, %xmm5
	mulpd	C, %xmm0
	movapd	%xmm2, %xmm7
	mulpd	C, %xmm2

	mulpd	C, %xmm4
	mulpd	S, %xmm5
	mulpd	C, %xmm6
	mulpd	S, %xmm7

	addpd	%xmm1, %xmm0
	movapd	16 * SIZE(Y), %xmm1
	addpd	%xmm3, %xmm2
	movapd	18 * SIZE(Y), %xmm3
	subpd	%xmm5, %xmm4
	subpd	%xmm7, %xmm6

	movapd	%xmm0,  8 * SIZE(X)
	movapd	16 * SIZE(X), %xmm0
	movapd	%xmm2, 10 * SIZE(X)
	movapd	18 * SIZE(X), %xmm2
	movapd	%xmm4,  8 * SIZE(Y)
	movapd	%xmm6, 10 * SIZE(Y)

#if defined(PREFETCHW) && !defined(FETCH128)
	PREFETCHW (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	movapd	%xmm9, %xmm4
	mulpd	S, %xmm9
	movapd	%xmm8, %xmm5
	mulpd	C, %xmm8
	movapd	%xmm11, %xmm6
	mulpd	S, %xmm11
	movapd	%xmm10, %xmm7
	mulpd	C, %xmm10

	mulpd	C, %xmm4
	mulpd	S, %xmm5
	mulpd	C, %xmm6
	mulpd	S, %xmm7

	addpd	%xmm9,  %xmm8
	movapd	20 * SIZE(Y), %xmm9
	addpd	%xmm11, %xmm10
	movapd	22 * SIZE(Y), %xmm11
	subpd	%xmm5,  %xmm4
	subpd	%xmm7,  %xmm6

	movapd	%xmm8,  12 * SIZE(X)
	movapd	20 * SIZE(X), %xmm8
	movapd	%xmm10, 14 * SIZE(X)
	movapd	22 * SIZE(X), %xmm10
	movapd	%xmm4,  12 * SIZE(Y)
	movapd	%xmm6,  14 * SIZE(Y)

	addq	$16 * SIZE, X
	addq	$16 * SIZE, Y

	decq	%rax
	jg	.L41
	ALIGN_3

.L42:
	movapd	%xmm1, %xmm4
	mulpd	S, %xmm1
	movapd	%xmm3, %xmm6
	mulpd	S, %xmm3
	movapd	%xmm0, %xmm5
	mulpd	C, %xmm0
	movapd	%xmm2, %xmm7
	mulpd	C, %xmm2

	mulpd	C, %xmm4
	mulpd	S, %xmm5
	mulpd	C, %xmm6
	mulpd	S, %xmm7

	addpd	%xmm1, %xmm0
	movapd	 8 * SIZE(Y), %xmm1
	addpd	%xmm3, %xmm2
	movapd	10 * SIZE(Y), %xmm3
	subpd	%xmm5, %xmm4
	subpd	%xmm7, %xmm6

	movapd	%xmm0, 0 * SIZE(X)
	movapd	 8 * SIZE(X), %xmm0
	movapd	%xmm2, 2 * SIZE(X)
	movapd	10 * SIZE(X), %xmm2

	movapd	%xmm4, 0 * SIZE(Y)
	movapd	%xmm6, 2 * SIZE(Y)

	movapd	%xmm9, %xmm4
	mulpd	S, %xmm9
	movapd	%xmm8, %xmm5
	mulpd	C, %xmm8
	movapd	%xmm11, %xmm6
	mulpd	S, %xmm11
	movapd	%xmm10, %xmm7
	mulpd	C, %xmm10

	mulpd	C, %xmm4
	mulpd	S, %xmm5
	mulpd	C, %xmm6
	mulpd	S, %xmm7

	addpd	%xmm9,  %xmm8
	movapd	12 * SIZE(Y), %xmm9
	addpd	%xmm11, %xmm10
	movapd	14 * SIZE(Y), %xmm11
	subpd	%xmm5,  %xmm4
	subpd	%xmm7,  %xmm6

	movapd	%xmm8, 4 * SIZE(X)
	movapd	12 * SIZE(X), %xmm8
	movapd	%xmm10,6 * SIZE(X)
	movapd	14 * SIZE(X), %xmm10
	movapd	%xmm4, 4 * SIZE(Y)
	movapd	%xmm6, 6 * SIZE(Y)

	movapd	%xmm1, %xmm4
	mulpd	S, %xmm1
	movapd	%xmm3, %xmm6
	mulpd	S, %xmm3
	movapd	%xmm0, %xmm5
	mulpd	C, %xmm0
	movapd	%xmm2, %xmm7
	mulpd	C, %xmm2

	mulpd	C, %xmm4
	mulpd	S, %xmm5
	mulpd	C, %xmm6
	mulpd	S, %xmm7

	addpd	%xmm1, %xmm0
	addpd	%xmm3, %xmm2
	subpd	%xmm5, %xmm4
	subpd	%xmm7, %xmm6

	movapd	%xmm0,  8 * SIZE(X)
	movapd	%xmm2, 10 * SIZE(X)
	movapd	%xmm4,  8 * SIZE(Y)
	movapd	%xmm6, 10 * SIZE(Y)

	movapd	%xmm9, %xmm4
	mulpd	S, %xmm9
	movapd	%xmm8, %xmm5
	mulpd	C, %xmm8
	movapd	%xmm11, %xmm6
	mulpd	S, %xmm11
	movapd	%xmm10, %xmm7
	mulpd	C, %xmm10

	mulpd	C, %xmm4
	mulpd	S, %xmm5
	mulpd	C, %xmm6
	mulpd	S, %xmm7

	addpd	%xmm9,  %xmm8
	addpd	%xmm11, %xmm10
	subpd	%xmm5,  %xmm4
	subpd	%xmm7,  %xmm6

	movapd	%xmm8,  12 * SIZE(X)
	movapd	%xmm10, 14 * SIZE(X)
	movapd	%xmm4,  12 * SIZE(Y)
	movapd	%xmm6,  14 * SIZE(Y)

	addq	$16 * SIZE, X
	addq	$16 * SIZE, Y
	ALIGN_3

.L44:
	testq	$4, N
	jle	.L45

	movapd	0 * SIZE(Y), %xmm1
	movapd	0 * SIZE(X), %xmm0
	movapd	2 * SIZE(Y), %xmm3
	movapd	2 * SIZE(X), %xmm2

	movapd	%xmm1, %xmm4
	movapd	%xmm0, %xmm5
	movapd	%xmm3, %xmm6
	movapd	%xmm2, %xmm7

	mulpd	C, %xmm0
	mulpd	S, %xmm1
	mulpd	C, %xmm2
	mulpd	S, %xmm3

	mulpd	C, %xmm4
	mulpd	S, %xmm5
	mulpd	C, %xmm6
	mulpd	S, %xmm7

	addpd	%xmm1, %xmm0
	addpd	%xmm3, %xmm2
	subpd	%xmm5, %xmm4
	subpd	%xmm7, %xmm6

	movapd	%xmm0, 0 * SIZE(X)
	movapd	%xmm2, 2 * SIZE(X)
	movapd	%xmm4, 0 * SIZE(Y)
	movapd	%xmm6, 2 * SIZE(Y)

	movapd	4 * SIZE(Y), %xmm1
	movapd	4 * SIZE(X), %xmm0
	movapd	6 * SIZE(Y), %xmm3
	movapd	6 * SIZE(X), %xmm2

	movapd	%xmm1, %xmm4
	movapd	%xmm0, %xmm5
	movapd	%xmm3, %xmm6
	movapd	%xmm2, %xmm7

	mulpd	C, %xmm0
	mulpd	S, %xmm1
	mulpd	C, %xmm2
	mulpd	S, %xmm3

	mulpd	C, %xmm4
	mulpd	S, %xmm5
	mulpd	C, %xmm6
	mulpd	S, %xmm7

	addpd	%xmm1, %xmm0
	addpd	%xmm3, %xmm2
	subpd	%xmm5, %xmm4
	subpd	%xmm7, %xmm6

	movapd	%xmm0, 4 * SIZE(X)
	movapd	%xmm2, 6 * SIZE(X)
	movapd	%xmm4, 4 * SIZE(Y)
	movapd	%xmm6, 6 * SIZE(Y)

	addq	$8 * SIZE, X
	addq	$8 * SIZE, Y
	ALIGN_3

.L45:
	testq	$2, N
	jle	.L46

	movapd	0 * SIZE(Y), %xmm1
	movapd	0 * SIZE(X), %xmm0
	movapd	2 * SIZE(Y), %xmm3
	movapd	2 * SIZE(X), %xmm2

	movapd	%xmm1, %xmm4
	movapd	%xmm0, %xmm5
	movapd	%xmm3, %xmm6
	movapd	%xmm2, %xmm7

	mulpd	C, %xmm0
	mulpd	S, %xmm1
	mulpd	C, %xmm2
	mulpd	S, %xmm3

	mulpd	C, %xmm4
	mulpd	S, %xmm5
	mulpd	C, %xmm6
	mulpd	S, %xmm7

	addpd	%xmm1, %xmm0
	addpd	%xmm3, %xmm2
	subpd	%xmm5, %xmm4
	subpd	%xmm7, %xmm6

	movapd	%xmm0, 0 * SIZE(X)
	movapd	%xmm2, 2 * SIZE(X)
	movapd	%xmm4, 0 * SIZE(Y)
	movapd	%xmm6, 2 * SIZE(Y)

	addq	$4 * SIZE, X
	addq	$4 * SIZE, Y
	ALIGN_3

.L46:
	testq	$1, N
	jle	.L47

	movapd	0 * SIZE(Y), %xmm1
	movapd	0 * SIZE(X), %xmm0

	movapd	%xmm1, %xmm2
	movapd	%xmm0, %xmm3

	mulpd	C, %xmm0
	mulpd	S, %xmm1

	mulpd	C, %xmm2
	mulpd	S, %xmm3

	addpd	%xmm1, %xmm0
	subpd	%xmm3, %xmm2

	movapd	%xmm0, 0 * SIZE(X)
	movapd	%xmm2, 0 * SIZE(Y)
	addq	$2 * SIZE, Y
	addq	$2 * SIZE, X
	ALIGN_3

.L47:
	movsd	0 * SIZE(Y), %xmm1
	movsd	0 * SIZE(X), %xmm0

	movapd	%xmm1, %xmm2
	movapd	%xmm0, %xmm3

	mulsd	C, %xmm0
	mulsd	S, %xmm1

	mulsd	C, %xmm2
	mulsd	S, %xmm3

	addsd	%xmm1, %xmm0
	subsd	%xmm3, %xmm2

	movsd	%xmm0, 0 * SIZE(X)
	movsd	%xmm2, 0 * SIZE(Y)
	jmp	.L999
	ALIGN_3

.L50:
	movq	N,  %rax
	sarq	$2, %rax
	jle	.L55
	ALIGN_3

.L53:
	movsd	0 * SIZE(Y), %xmm1
	movhps	1 * SIZE(Y), %xmm1
	movsd	0 * SIZE(X), %xmm0
	movhps	1 * SIZE(X), %xmm0

	movapd	%xmm1, %xmm2
	movapd	%xmm0, %xmm3

	mulpd	C, %xmm0
	mulpd	S, %xmm1

	mulpd	C, %xmm2
	mulpd	S, %xmm3

	addpd	%xmm1, %xmm0
	subpd	%xmm3, %xmm2

	movlps	%xmm0, 0 * SIZE(X)
	movhps	%xmm0, 1 * SIZE(X)
	movlps	%xmm2, 0 * SIZE(Y)
	movhps	%xmm2, 1 * SIZE(Y)

	addq	INCX, X
	addq	INCY, Y

	movsd	0 * SIZE(Y), %xmm1
	movhps	1 * SIZE(Y), %xmm1
	movsd	0 * SIZE(X), %xmm0
	movhps	1 * SIZE(X), %xmm0

	movapd	%xmm1, %xmm2
	movapd	%xmm0, %xmm3

	mulpd	C, %xmm0
	mulpd	S, %xmm1

	mulpd	C, %xmm2
	mulpd	S, %xmm3

	addpd	%xmm1, %xmm0
	subpd	%xmm3, %xmm2

	movlps	%xmm0, 0 * SIZE(X)
	movhps	%xmm0, 1 * SIZE(X)
	movlps	%xmm2, 0 * SIZE(Y)
	movhps	%xmm2, 1 * SIZE(Y)

	addq	INCX, X
	addq	INCY, Y

	movsd	0 * SIZE(Y), %xmm1
	movhps	1 * SIZE(Y), %xmm1
	movsd	0 * SIZE(X), %xmm0
	movhps	1 * SIZE(X), %xmm0

	movapd	%xmm1, %xmm2
	movapd	%xmm0, %xmm3

	mulpd	C, %xmm0
	mulpd	S, %xmm1

	mulpd	C, %xmm2
	mulpd	S, %xmm3

	addpd	%xmm1, %xmm0
	subpd	%xmm3, %xmm2

	movlps	%xmm0, 0 * SIZE(X)
	movhps	%xmm0, 1 * SIZE(X)
	movlps	%xmm2, 0 * SIZE(Y)
	movhps	%xmm2, 1 * SIZE(Y)

	addq	INCX, X
	addq	INCY, Y

	movsd	0 * SIZE(Y), %xmm1
	movhps	1 * SIZE(Y), %xmm1
	movsd	0 * SIZE(X), %xmm0
	movhps	1 * SIZE(X), %xmm0

	movapd	%xmm1, %xmm2
	movapd	%xmm0, %xmm3

	mulpd	C, %xmm0
	mulpd	S, %xmm1

	mulpd	C, %xmm2
	mulpd	S, %xmm3

	addpd	%xmm1, %xmm0
	subpd	%xmm3, %xmm2

	movlps	%xmm0, 0 * SIZE(X)
	movhps	%xmm0, 1 * SIZE(X)
	movlps	%xmm2, 0 * SIZE(Y)
	movhps	%xmm2, 1 * SIZE(Y)

	addq	INCX, X
	addq	INCY, Y

	decq	%rax
	jg	.L53
	ALIGN_3

.L55:
	movq	N,  %rax
	andq	$3, %rax
	jle	.L999
	ALIGN_3

.L56:
	movsd	0 * SIZE(Y), %xmm1
	movhps	1 * SIZE(Y), %xmm1
	movsd	0 * SIZE(X), %xmm0
	movhps	1 * SIZE(X), %xmm0

	movapd	%xmm1, %xmm2
	movapd	%xmm0, %xmm3

	mulpd	C, %xmm0
	mulpd	S, %xmm1

	mulpd	C, %xmm2
	mulpd	S, %xmm3

	addpd	%xmm1, %xmm0
	subpd	%xmm3, %xmm2

	movlps	%xmm0, 0 * SIZE(X)
	movhps	%xmm0, 1 * SIZE(X)
	movlps	%xmm2, 0 * SIZE(Y)
	movhps	%xmm2, 1 * SIZE(Y)

	addq	INCX, X
	addq	INCY, Y

	decq	%rax
	jg	.L56
	ALIGN_3

.L999:
	RESTOREREGISTERS

	ret

	EPILOGUE
