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

	leaq	(, INCX, SIZE), INCX
	leaq	(, INCY, SIZE), INCY

	pshufd	$0x44, %xmm0, C
	pshufd	$0x44, %xmm1, S

	cmpq	$0, N
	jle	.L999

	cmpq	$SIZE, INCX
	jne	.L50
	cmpq	$SIZE, INCY
	jne	.L50

	testq	$SIZE, X
	je	.L10

	movsd	0 * SIZE(Y), %xmm1
	movsd	0 * SIZE(X), %xmm0

	movaps	%xmm1, %xmm2
	movaps	%xmm0, %xmm3

	mulsd	C, %xmm0
	mulsd	S, %xmm1

	mulsd	C, %xmm2
	mulsd	S, %xmm3

	addsd	%xmm1, %xmm0
	subsd	%xmm3, %xmm2

	movsd	%xmm0, 0 * SIZE(X)
	movsd	%xmm2, 0 * SIZE(Y)

	addq	$1 * SIZE, X
	addq	$1 * SIZE, Y
	decq	N
	jle	.L999
	ALIGN_2

.L10:
	testq	$SIZE, Y
	jne	.L20

	movq	N,  %rax
	sarq	$4, %rax
	jle	.L14

	movaps	0 * SIZE(Y), %xmm1
	movaps	2 * SIZE(Y), %xmm3
	movaps	4 * SIZE(Y), %xmm9
	movaps	6 * SIZE(Y), %xmm11

	movaps	0 * SIZE(X), %xmm0
	movaps	2 * SIZE(X), %xmm2
	movaps	4 * SIZE(X), %xmm8
	movaps	6 * SIZE(X), %xmm10

	decq	%rax
	jle	.L12
	ALIGN_3

.L11:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movaps	%xmm1, %xmm4
	mulpd	S, %xmm1
	movaps	%xmm3, %xmm6
	mulpd	S, %xmm3
	movaps	%xmm0, %xmm5
	mulpd	C, %xmm0
	movaps	%xmm2, %xmm7
	mulpd	C, %xmm2

	mulpd	C, %xmm4
	mulpd	S, %xmm5
	mulpd	C, %xmm6
	mulpd	S, %xmm7

	addpd	%xmm1, %xmm0
	movaps	 8 * SIZE(Y), %xmm1
	addpd	%xmm3, %xmm2
	movaps	10 * SIZE(Y), %xmm3
	subpd	%xmm5, %xmm4
	subpd	%xmm7, %xmm6

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	movaps	%xmm0, 0 * SIZE(X)
	movaps	 8 * SIZE(X), %xmm0
	movaps	%xmm2, 2 * SIZE(X)
	movaps	10 * SIZE(X), %xmm2
	movaps	%xmm4, 0 * SIZE(Y)
	movaps	%xmm6, 2 * SIZE(Y)

	movaps	%xmm9, %xmm4
	mulpd	S, %xmm9
	movaps	%xmm8, %xmm5
	mulpd	C, %xmm8
	movaps	%xmm11, %xmm6
	mulpd	S, %xmm11
	movaps	%xmm10, %xmm7
	mulpd	C, %xmm10

	mulpd	C, %xmm4
	mulpd	S, %xmm5
	mulpd	C, %xmm6
	mulpd	S, %xmm7

	addpd	%xmm9,  %xmm8
	movaps	12 * SIZE(Y), %xmm9
	addpd	%xmm11, %xmm10
	movaps	14 * SIZE(Y), %xmm11
	subpd	%xmm5,  %xmm4
	subpd	%xmm7,  %xmm6

	movaps	%xmm8, 4 * SIZE(X)
	movaps	12 * SIZE(X), %xmm8
	movaps	%xmm10,6 * SIZE(X)
	movaps	14 * SIZE(X), %xmm10
	movaps	%xmm4, 4 * SIZE(Y)
	movaps	%xmm6, 6 * SIZE(Y)

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE + 64) - PREOFFSET(X)
#endif

	movaps	%xmm1, %xmm4
	mulpd	S, %xmm1
	movaps	%xmm3, %xmm6
	mulpd	S, %xmm3
	movaps	%xmm0, %xmm5
	mulpd	C, %xmm0
	movaps	%xmm2, %xmm7
	mulpd	C, %xmm2

	mulpd	C, %xmm4
	mulpd	S, %xmm5
	mulpd	C, %xmm6
	mulpd	S, %xmm7

	addpd	%xmm1, %xmm0
	movaps	16 * SIZE(Y), %xmm1
	addpd	%xmm3, %xmm2
	movaps	18 * SIZE(Y), %xmm3
	subpd	%xmm5, %xmm4
	subpd	%xmm7, %xmm6

	movaps	%xmm0,  8 * SIZE(X)
	movaps	16 * SIZE(X), %xmm0
	movaps	%xmm2, 10 * SIZE(X)
	movaps	18 * SIZE(X), %xmm2
	movaps	%xmm4,  8 * SIZE(Y)
	movaps	%xmm6, 10 * SIZE(Y)

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE + 64) - PREOFFSET(Y)
#endif

	movaps	%xmm9, %xmm4
	mulpd	S, %xmm9
	movaps	%xmm8, %xmm5
	mulpd	C, %xmm8
	movaps	%xmm11, %xmm6
	mulpd	S, %xmm11
	movaps	%xmm10, %xmm7
	mulpd	C, %xmm10

	mulpd	C, %xmm4
	mulpd	S, %xmm5
	mulpd	C, %xmm6
	mulpd	S, %xmm7

	addpd	%xmm9,  %xmm8
	movaps	20 * SIZE(Y), %xmm9
	addpd	%xmm11, %xmm10
	movaps	22 * SIZE(Y), %xmm11
	subpd	%xmm5,  %xmm4
	subpd	%xmm7,  %xmm6

	movaps	%xmm8,  12 * SIZE(X)
	movaps	20 * SIZE(X), %xmm8
	movaps	%xmm10, 14 * SIZE(X)
	movaps	22 * SIZE(X), %xmm10
	movaps	%xmm4,  12 * SIZE(Y)
	movaps	%xmm6,  14 * SIZE(Y)

	addq	$16 * SIZE, X
	addq	$16 * SIZE, Y

	decq	%rax
	jg	.L11
	ALIGN_3

.L12:
	movaps	%xmm1, %xmm4
	mulpd	S, %xmm1
	movaps	%xmm3, %xmm6
	mulpd	S, %xmm3
	movaps	%xmm0, %xmm5
	mulpd	C, %xmm0
	movaps	%xmm2, %xmm7
	mulpd	C, %xmm2

	mulpd	C, %xmm4
	mulpd	S, %xmm5
	mulpd	C, %xmm6
	mulpd	S, %xmm7

	addpd	%xmm1, %xmm0
	movaps	 8 * SIZE(Y), %xmm1
	addpd	%xmm3, %xmm2
	movaps	10 * SIZE(Y), %xmm3
	subpd	%xmm5, %xmm4
	subpd	%xmm7, %xmm6

	movaps	%xmm0, 0 * SIZE(X)
	movaps	 8 * SIZE(X), %xmm0
	movaps	%xmm2, 2 * SIZE(X)
	movaps	10 * SIZE(X), %xmm2

	movaps	%xmm4, 0 * SIZE(Y)
	movaps	%xmm6, 2 * SIZE(Y)

	movaps	%xmm9, %xmm4
	mulpd	S, %xmm9
	movaps	%xmm8, %xmm5
	mulpd	C, %xmm8
	movaps	%xmm11, %xmm6
	mulpd	S, %xmm11
	movaps	%xmm10, %xmm7
	mulpd	C, %xmm10

	mulpd	C, %xmm4
	mulpd	S, %xmm5
	mulpd	C, %xmm6
	mulpd	S, %xmm7

	addpd	%xmm9,  %xmm8
	movaps	12 * SIZE(Y), %xmm9
	addpd	%xmm11, %xmm10
	movaps	14 * SIZE(Y), %xmm11
	subpd	%xmm5,  %xmm4
	subpd	%xmm7,  %xmm6

	movaps	%xmm8, 4 * SIZE(X)
	movaps	12 * SIZE(X), %xmm8
	movaps	%xmm10,6 * SIZE(X)
	movaps	14 * SIZE(X), %xmm10
	movaps	%xmm4, 4 * SIZE(Y)
	movaps	%xmm6, 6 * SIZE(Y)

	movaps	%xmm1, %xmm4
	mulpd	S, %xmm1
	movaps	%xmm3, %xmm6
	mulpd	S, %xmm3
	movaps	%xmm0, %xmm5
	mulpd	C, %xmm0
	movaps	%xmm2, %xmm7
	mulpd	C, %xmm2

	mulpd	C, %xmm4
	mulpd	S, %xmm5
	mulpd	C, %xmm6
	mulpd	S, %xmm7

	addpd	%xmm1, %xmm0
	addpd	%xmm3, %xmm2
	subpd	%xmm5, %xmm4
	subpd	%xmm7, %xmm6

	movaps	%xmm0,  8 * SIZE(X)
	movaps	%xmm2, 10 * SIZE(X)
	movaps	%xmm4,  8 * SIZE(Y)
	movaps	%xmm6, 10 * SIZE(Y)

	movaps	%xmm9, %xmm4
	mulpd	S, %xmm9
	movaps	%xmm8, %xmm5
	mulpd	C, %xmm8
	movaps	%xmm11, %xmm6
	mulpd	S, %xmm11
	movaps	%xmm10, %xmm7
	mulpd	C, %xmm10

	mulpd	C, %xmm4
	mulpd	S, %xmm5
	mulpd	C, %xmm6
	mulpd	S, %xmm7

	addpd	%xmm9,  %xmm8
	addpd	%xmm11, %xmm10
	subpd	%xmm5,  %xmm4
	subpd	%xmm7,  %xmm6

	movaps	%xmm8,  12 * SIZE(X)
	movaps	%xmm10, 14 * SIZE(X)
	movaps	%xmm4,  12 * SIZE(Y)
	movaps	%xmm6,  14 * SIZE(Y)

	addq	$16 * SIZE, X
	addq	$16 * SIZE, Y
	ALIGN_3


.L14:
	testq	$15, N
	jle	.L999

	testq	$8, N
	jle	.L15

	movaps	0 * SIZE(Y), %xmm1
	movaps	0 * SIZE(X), %xmm0
	movaps	2 * SIZE(Y), %xmm3
	movaps	2 * SIZE(X), %xmm2

	movaps	%xmm1, %xmm4
	movaps	%xmm0, %xmm5
	movaps	%xmm3, %xmm6
	movaps	%xmm2, %xmm7

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

	movaps	%xmm0, 0 * SIZE(X)
	movaps	%xmm2, 2 * SIZE(X)
	movaps	%xmm4, 0 * SIZE(Y)
	movaps	%xmm6, 2 * SIZE(Y)

	movaps	4 * SIZE(Y), %xmm1
	movaps	4 * SIZE(X), %xmm0
	movaps	6 * SIZE(Y), %xmm3
	movaps	6 * SIZE(X), %xmm2

	movaps	%xmm1, %xmm4
	movaps	%xmm0, %xmm5
	movaps	%xmm3, %xmm6
	movaps	%xmm2, %xmm7

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

	movaps	%xmm0, 4 * SIZE(X)
	movaps	%xmm2, 6 * SIZE(X)
	movaps	%xmm4, 4 * SIZE(Y)
	movaps	%xmm6, 6 * SIZE(Y)

	addq	$8 * SIZE, X
	addq	$8 * SIZE, Y
	ALIGN_3

.L15:
	testq	$4, N
	jle	.L16

	movaps	0 * SIZE(Y), %xmm1
	movaps	0 * SIZE(X), %xmm0
	movaps	2 * SIZE(Y), %xmm3
	movaps	2 * SIZE(X), %xmm2

	movaps	%xmm1, %xmm4
	movaps	%xmm0, %xmm5
	movaps	%xmm3, %xmm6
	movaps	%xmm2, %xmm7

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

	movaps	%xmm0, 0 * SIZE(X)
	movaps	%xmm2, 2 * SIZE(X)
	movaps	%xmm4, 0 * SIZE(Y)
	movaps	%xmm6, 2 * SIZE(Y)

	addq	$4 * SIZE, X
	addq	$4 * SIZE, Y
	ALIGN_3

.L16:
	testq	$2, N
	jle	.L17

	movaps	0 * SIZE(Y), %xmm1
	movaps	0 * SIZE(X), %xmm0

	movaps	%xmm1, %xmm2
	movaps	%xmm0, %xmm3

	mulpd	C, %xmm0
	mulpd	S, %xmm1

	mulpd	C, %xmm2
	mulpd	S, %xmm3

	addpd	%xmm1, %xmm0
	subpd	%xmm3, %xmm2

	movaps	%xmm0, 0 * SIZE(X)
	movaps	%xmm2, 0 * SIZE(Y)

	addq	$2 * SIZE, X
	addq	$2 * SIZE, Y
	ALIGN_3

.L17:
	testq	$1, N
	jle	.L999

	movsd	0 * SIZE(Y), %xmm1
	movsd	0 * SIZE(X), %xmm0

	movaps	%xmm1, %xmm2
	movaps	%xmm0, %xmm3

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

.L20:
	movaps	-1 * SIZE(Y), %xmm1

	movq	N,  %rax
	sarq	$4, %rax
	jle	.L24
	ALIGN_3

.L21:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movaps	1 * SIZE(Y), %xmm3
	movaps	3 * SIZE(Y), %xmm8
	movaps	0 * SIZE(X), %xmm0
	movaps	2 * SIZE(X), %xmm2

	SHUFPD_1 %xmm3, %xmm1
	SHUFPD_1 %xmm8, %xmm3

	movaps	%xmm1, %xmm4
	movaps	%xmm0, %xmm5
	movaps	%xmm3, %xmm6
	movaps	%xmm2, %xmm7

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

	movaps	%xmm0, 0 * SIZE(X)
	movaps	%xmm2, 2 * SIZE(X)

	movlpd	%xmm4, 0 * SIZE(Y)
	movhps	%xmm4, 1 * SIZE(Y)
	movlpd	%xmm6, 2 * SIZE(Y)
	movhps	%xmm6, 3 * SIZE(Y)

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	movaps	5 * SIZE(Y), %xmm9
	movaps	7 * SIZE(Y), %xmm1
	movaps	4 * SIZE(X), %xmm0
	movaps	6 * SIZE(X), %xmm2

	SHUFPD_1 %xmm9, %xmm8
	SHUFPD_1 %xmm1, %xmm9

	movaps	%xmm8, %xmm4
	movaps	%xmm0, %xmm5
	movaps	%xmm9, %xmm6
	movaps	%xmm2, %xmm7

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

	movaps	%xmm0, 4 * SIZE(X)
	movaps	%xmm2, 6 * SIZE(X)
	movlpd	%xmm4, 4 * SIZE(Y)
	movhps	%xmm4, 5 * SIZE(Y)
	movlpd	%xmm6, 6 * SIZE(Y)
	movhps	%xmm6, 7 * SIZE(Y)

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE + 64) - PREOFFSET(X)
#endif

	movaps	 9 * SIZE(Y), %xmm3
	movaps	11 * SIZE(Y), %xmm8
	movaps	 8 * SIZE(X), %xmm0
	movaps	10 * SIZE(X), %xmm2

	SHUFPD_1 %xmm3, %xmm1
	SHUFPD_1 %xmm8, %xmm3

	movaps	%xmm1, %xmm4
	movaps	%xmm0, %xmm5
	movaps	%xmm3, %xmm6
	movaps	%xmm2, %xmm7

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

	movaps	%xmm0,  8 * SIZE(X)
	movaps	%xmm2, 10 * SIZE(X)
	movlpd	%xmm4,  8 * SIZE(Y)
	movhps	%xmm4,  9 * SIZE(Y)
	movlpd	%xmm6, 10 * SIZE(Y)
	movhps	%xmm6, 11 * SIZE(Y)

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE + 64) - PREOFFSET(Y)
#endif

	movaps	13 * SIZE(Y), %xmm9
	movaps	15 * SIZE(Y), %xmm1
	movaps	12 * SIZE(X), %xmm0
	movaps	14 * SIZE(X), %xmm2

	SHUFPD_1 %xmm9, %xmm8
	SHUFPD_1 %xmm1, %xmm9

	movaps	%xmm8, %xmm4
	movaps	%xmm0, %xmm5
	movaps	%xmm9, %xmm6
	movaps	%xmm2, %xmm7

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

	movaps	%xmm0, 12 * SIZE(X)
	movaps	%xmm2, 14 * SIZE(X)
	movlpd	%xmm4, 12 * SIZE(Y)
	movhps	%xmm4, 13 * SIZE(Y)
	movlpd	%xmm6, 14 * SIZE(Y)
	movhps	%xmm6, 15 * SIZE(Y)

	addq	$16 * SIZE, X
	addq	$16 * SIZE, Y
	decq	%rax
	jg	.L21
	ALIGN_3

.L24:
	testq	$15, N
	jle	.L999

	testq	$8, N
	jle	.L25

	movaps	1 * SIZE(Y), %xmm3
	movaps	3 * SIZE(Y), %xmm8
	movaps	0 * SIZE(X), %xmm0
	movaps	2 * SIZE(X), %xmm2

	SHUFPD_1 %xmm3, %xmm1
	SHUFPD_1 %xmm8, %xmm3

	movaps	%xmm1, %xmm4
	movaps	%xmm0, %xmm5
	movaps	%xmm3, %xmm6
	movaps	%xmm2, %xmm7

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

	movaps	%xmm0, 0 * SIZE(X)
	movaps	%xmm2, 2 * SIZE(X)
	movlpd	%xmm4, 0 * SIZE(Y)
	movhps	%xmm4, 1 * SIZE(Y)
	movlpd	%xmm6, 2 * SIZE(Y)
	movhps	%xmm6, 3 * SIZE(Y)

	movaps	5 * SIZE(Y), %xmm9
	movaps	7 * SIZE(Y), %xmm1
	movaps	4 * SIZE(X), %xmm0
	movaps	6 * SIZE(X), %xmm2

	SHUFPD_1 %xmm9, %xmm8
	SHUFPD_1 %xmm1, %xmm9

	movaps	%xmm8, %xmm4
	movaps	%xmm0, %xmm5
	movaps	%xmm9, %xmm6
	movaps	%xmm2, %xmm7

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

	movaps	%xmm0, 4 * SIZE(X)
	movaps	%xmm2, 6 * SIZE(X)
	movlpd	%xmm4, 4 * SIZE(Y)
	movhps	%xmm4, 5 * SIZE(Y)
	movlpd	%xmm6, 6 * SIZE(Y)
	movhps	%xmm6, 7 * SIZE(Y)

	addq	$8 * SIZE, X
	addq	$8 * SIZE, Y
	ALIGN_3

.L25:
	testq	$4, N
	jle	.L26

	movaps	1 * SIZE(Y), %xmm3
	movaps	3 * SIZE(Y), %xmm8
	movaps	0 * SIZE(X), %xmm0
	movaps	2 * SIZE(X), %xmm2

	SHUFPD_1 %xmm3, %xmm1
	SHUFPD_1 %xmm8, %xmm3

	movaps	%xmm1, %xmm4
	movaps	%xmm0, %xmm5
	movaps	%xmm3, %xmm6
	movaps	%xmm2, %xmm7

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

	movaps	%xmm0, 0 * SIZE(X)
	movaps	%xmm2, 2 * SIZE(X)
	movlpd	%xmm4, 0 * SIZE(Y)
	movhps	%xmm4, 1 * SIZE(Y)
	movlpd	%xmm6, 2 * SIZE(Y)
	movhps	%xmm6, 3 * SIZE(Y)
	movaps	%xmm8, %xmm1

	addq	$4 * SIZE, X
	addq	$4 * SIZE, Y
	ALIGN_3

.L26:
	testq	$2, N
	jle	.L27

	movaps	1 * SIZE(Y), %xmm4
	movaps	0 * SIZE(X), %xmm0

	SHUFPD_1 %xmm4, %xmm1
	movaps	%xmm1, %xmm2
	movaps	%xmm0, %xmm3

	mulpd	C, %xmm0
	mulpd	S, %xmm1

	mulpd	C, %xmm2
	mulpd	S, %xmm3

	addpd	%xmm1, %xmm0
	subpd	%xmm3, %xmm2

	movaps	%xmm0, 0 * SIZE(X)
	movlpd	%xmm2, 0 * SIZE(Y)
	movhps	%xmm2, 1 * SIZE(Y)
	movaps	%xmm4, %xmm1

	addq	$2 * SIZE, X
	addq	$2 * SIZE, Y
	ALIGN_3

.L27:
	testq	$1, N
	jle	.L999

	unpckhpd %xmm1, %xmm1
	movsd	0 * SIZE(X), %xmm0

	movaps	%xmm1, %xmm2
	movaps	%xmm0, %xmm3

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
	cmpq	$0, INCX
	je  .L56
	cmpq	$0, INCY
	je  .L56
	sarq	$2, %rax
	jle	.L55
	ALIGN_3

.L53:
	movsd	(Y), %xmm1
	movhps	(Y, INCY), %xmm1
	movsd	(X), %xmm0
	movhps	(X, INCX), %xmm0

	movaps	%xmm1, %xmm2
	movaps	%xmm0, %xmm3

	mulpd	C, %xmm0
	mulpd	S, %xmm1

	mulpd	C, %xmm2
	mulpd	S, %xmm3

	addpd	%xmm1, %xmm0
	subpd	%xmm3, %xmm2

	movlpd	%xmm0, (X)
	movhps	%xmm0, (X, INCX)
	movlpd	%xmm2, (Y)
	movhps	%xmm2, (Y, INCY)

	leaq	(X, INCX, 2), X
	leaq	(Y, INCY, 2), Y

	movsd	(Y), %xmm1
	movhps	(Y, INCY), %xmm1
	movsd	(X), %xmm0
	movhps	(X, INCX), %xmm0

	movaps	%xmm1, %xmm2
	movaps	%xmm0, %xmm3

	mulpd	C, %xmm0
	mulpd	S, %xmm1

	mulpd	C, %xmm2
	mulpd	S, %xmm3

	addpd	%xmm1, %xmm0
	subpd	%xmm3, %xmm2

	movlpd	%xmm0, (X)
	movhps	%xmm0, (X, INCX)
	movlpd	%xmm2, (Y)
	movhps	%xmm2, (Y, INCY)

	leaq	(X, INCX, 2), X
	leaq	(Y, INCY, 2), Y

	decq	%rax
	jg	.L53
	ALIGN_3

.L55:
	movq	N,  %rax
	andq	$3, %rax
	jle	.L999
	ALIGN_3

.L56:
	movsd	(Y), %xmm1
	movsd	(X), %xmm0

	movaps	%xmm1, %xmm2
	movaps	%xmm0, %xmm3

	mulsd	C, %xmm0
	mulsd	S, %xmm1

	mulsd	C, %xmm2
	mulsd	S, %xmm3

	addsd	%xmm1, %xmm0
	subsd	%xmm3, %xmm2

	movsd	%xmm0, (X)
	movsd	%xmm2, (Y)

	addq	INCX, X
	addq	INCY, Y

	decq	%rax
	jg	.L56
	ALIGN_3

.L999:
	RESTOREREGISTERS

	ret

	EPILOGUE
