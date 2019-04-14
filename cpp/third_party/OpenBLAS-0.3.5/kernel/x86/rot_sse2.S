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

#define STACK	12
#define ARGS     0

#define STACK_N		 4 + STACK + ARGS(%esp)
#define STACK_X		 8 + STACK + ARGS(%esp)
#define STACK_INCX	12 + STACK + ARGS(%esp)
#define STACK_Y		16 + STACK + ARGS(%esp)
#define STACK_INCY	20 + STACK + ARGS(%esp)
#define STACK_C		24 + STACK + ARGS(%esp)
#define STACK_S		32 + STACK + ARGS(%esp)

#define N	%ebx
#define X	%esi
#define INCX	%ecx
#define Y	%edi
#define INCY	%edx

#define	I	%eax

#include "l1param.h"

#define C	%xmm6
#define S	%xmm7

	PROLOGUE
	PROFCODE

	pushl	%edi
	pushl	%esi
	pushl	%ebx

	movl	STACK_N,     N
	movl	STACK_X,     X
	movl	STACK_INCX,  INCX
	movl	STACK_Y,     Y
	movl	STACK_INCY,  INCY

	leal	(, INCX, SIZE), INCX
	leal	(, INCY, SIZE), INCY

	movsd	STACK_C, C
	movsd	STACK_S, S

	pshufd	$0x44, C, C
	pshufd	$0x44, S, S

	cmpl	$0, N
	jle	.L999

	cmpl	$SIZE, INCX
	jne	.L50
	cmpl	$SIZE, INCY
	jne	.L50

	testl	$SIZE, X
	je	.L10

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

	addl	$1 * SIZE, X
	addl	$1 * SIZE, Y
	decl	N
	jle	.L999
	ALIGN_2

.L10:
	testl	$SIZE, Y
	jne	.L20

	movl	N,  I
	sarl	$4, I
	jle	.L14
	ALIGN_3

.L11:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

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

	movapd	2 * SIZE(Y), %xmm1
	movapd	2 * SIZE(X), %xmm0

	movapd	%xmm1, %xmm2
	movapd	%xmm0, %xmm3

	mulpd	C, %xmm0
	mulpd	S, %xmm1

	mulpd	C, %xmm2
	mulpd	S, %xmm3

	addpd	%xmm1, %xmm0
	subpd	%xmm3, %xmm2

	movapd	%xmm0, 2 * SIZE(X)
	movapd	%xmm2, 2 * SIZE(Y)

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	movapd	4 * SIZE(Y), %xmm1
	movapd	4 * SIZE(X), %xmm0

	movapd	%xmm1, %xmm2
	movapd	%xmm0, %xmm3

	mulpd	C, %xmm0
	mulpd	S, %xmm1

	mulpd	C, %xmm2
	mulpd	S, %xmm3

	addpd	%xmm1, %xmm0
	subpd	%xmm3, %xmm2

	movapd	%xmm0, 4 * SIZE(X)
	movapd	%xmm2, 4 * SIZE(Y)

	movapd	6 * SIZE(Y), %xmm1
	movapd	6 * SIZE(X), %xmm0

	movapd	%xmm1, %xmm2
	movapd	%xmm0, %xmm3

	mulpd	C, %xmm0
	mulpd	S, %xmm1

	mulpd	C, %xmm2
	mulpd	S, %xmm3

	addpd	%xmm1, %xmm0
	subpd	%xmm3, %xmm2

	movapd	%xmm0, 6 * SIZE(X)
	movapd	%xmm2, 6 * SIZE(Y)

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE + 64) - PREOFFSET(X)
#endif

	movapd	8 * SIZE(Y), %xmm1
	movapd	8 * SIZE(X), %xmm0

	movapd	%xmm1, %xmm2
	movapd	%xmm0, %xmm3

	mulpd	C, %xmm0
	mulpd	S, %xmm1

	mulpd	C, %xmm2
	mulpd	S, %xmm3

	addpd	%xmm1, %xmm0
	subpd	%xmm3, %xmm2

	movapd	%xmm0, 8 * SIZE(X)
	movapd	%xmm2, 8 * SIZE(Y)

	movapd	10 * SIZE(Y), %xmm1
	movapd	10 * SIZE(X), %xmm0

	movapd	%xmm1, %xmm2
	movapd	%xmm0, %xmm3

	mulpd	C, %xmm0
	mulpd	S, %xmm1

	mulpd	C, %xmm2
	mulpd	S, %xmm3

	addpd	%xmm1, %xmm0
	subpd	%xmm3, %xmm2

	movapd	%xmm0, 10 * SIZE(X)
	movapd	%xmm2, 10 * SIZE(Y)

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE + 64) - PREOFFSET(Y)
#endif

	movapd	12 * SIZE(Y), %xmm1
	movapd	12 * SIZE(X), %xmm0

	movapd	%xmm1, %xmm2
	movapd	%xmm0, %xmm3

	mulpd	C, %xmm0
	mulpd	S, %xmm1

	mulpd	C, %xmm2
	mulpd	S, %xmm3

	addpd	%xmm1, %xmm0
	subpd	%xmm3, %xmm2

	movapd	%xmm0, 12 * SIZE(X)
	movapd	%xmm2, 12 * SIZE(Y)

	movapd	14 * SIZE(Y), %xmm1
	movapd	14 * SIZE(X), %xmm0

	movapd	%xmm1, %xmm2
	movapd	%xmm0, %xmm3

	mulpd	C, %xmm0
	mulpd	S, %xmm1

	mulpd	C, %xmm2
	mulpd	S, %xmm3

	addpd	%xmm1, %xmm0
	subpd	%xmm3, %xmm2

	movapd	%xmm0, 14 * SIZE(X)
	movapd	%xmm2, 14 * SIZE(Y)

	addl	$16 * SIZE, X
	addl	$16 * SIZE, Y

	decl	I
	jg	.L11
	ALIGN_3

.L14:
	testl	$15, N
	jle	.L999

	testl	$8, N
	jle	.L15

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

	movapd	2 * SIZE(Y), %xmm1
	movapd	2 * SIZE(X), %xmm0

	movapd	%xmm1, %xmm2
	movapd	%xmm0, %xmm3

	mulpd	C, %xmm0
	mulpd	S, %xmm1

	mulpd	C, %xmm2
	mulpd	S, %xmm3

	addpd	%xmm1, %xmm0
	subpd	%xmm3, %xmm2

	movapd	%xmm0, 2 * SIZE(X)
	movapd	%xmm2, 2 * SIZE(Y)

	movapd	4 * SIZE(Y), %xmm1
	movapd	4 * SIZE(X), %xmm0

	movapd	%xmm1, %xmm2
	movapd	%xmm0, %xmm3

	mulpd	C, %xmm0
	mulpd	S, %xmm1

	mulpd	C, %xmm2
	mulpd	S, %xmm3

	addpd	%xmm1, %xmm0
	subpd	%xmm3, %xmm2

	movapd	%xmm0, 4 * SIZE(X)
	movapd	%xmm2, 4 * SIZE(Y)

	movapd	6 * SIZE(Y), %xmm1
	movapd	6 * SIZE(X), %xmm0

	movapd	%xmm1, %xmm2
	movapd	%xmm0, %xmm3

	mulpd	C, %xmm0
	mulpd	S, %xmm1

	mulpd	C, %xmm2
	mulpd	S, %xmm3

	addpd	%xmm1, %xmm0
	subpd	%xmm3, %xmm2

	movapd	%xmm0, 6 * SIZE(X)
	movapd	%xmm2, 6 * SIZE(Y)

	addl	$8 * SIZE, X
	addl	$8 * SIZE, Y
	ALIGN_3

.L15:
	testl	$4, N
	jle	.L16

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

	movapd	2 * SIZE(Y), %xmm1
	movapd	2 * SIZE(X), %xmm0

	movapd	%xmm1, %xmm2
	movapd	%xmm0, %xmm3

	mulpd	C, %xmm0
	mulpd	S, %xmm1

	mulpd	C, %xmm2
	mulpd	S, %xmm3

	addpd	%xmm1, %xmm0
	subpd	%xmm3, %xmm2

	movapd	%xmm0, 2 * SIZE(X)
	movapd	%xmm2, 2 * SIZE(Y)

	addl	$4 * SIZE, X
	addl	$4 * SIZE, Y
	ALIGN_3

.L16:
	testl	$2, N
	jle	.L17

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

	addl	$2 * SIZE, X
	addl	$2 * SIZE, Y
	ALIGN_3

.L17:
	testl	$1, N
	jle	.L999

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

.L20:
	movapd	-1 * SIZE(Y), %xmm1

	movl	N,  I
	sarl	$4, I
	jle	.L24
	ALIGN_3

.L21:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

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
	movlpd	%xmm2, 0 * SIZE(Y)
	movhpd	%xmm2, 1 * SIZE(Y)

	movapd	3 * SIZE(Y), %xmm1
	movapd	2 * SIZE(X), %xmm0

	SHUFPD_1 %xmm1, %xmm4
	movapd	%xmm4, %xmm2
	movapd	%xmm0, %xmm3

	mulpd	C, %xmm0
	mulpd	S, %xmm4

	mulpd	C, %xmm2
	mulpd	S, %xmm3

	addpd	%xmm4, %xmm0
	subpd	%xmm3, %xmm2

	movapd	%xmm0, 2 * SIZE(X)
	movlpd	%xmm2, 2 * SIZE(Y)
	movhpd	%xmm2, 3 * SIZE(Y)

	movapd	5 * SIZE(Y), %xmm4
	movapd	4 * SIZE(X), %xmm0

	SHUFPD_1 %xmm4, %xmm1
	movapd	%xmm1, %xmm2
	movapd	%xmm0, %xmm3

	mulpd	C, %xmm0
	mulpd	S, %xmm1

	mulpd	C, %xmm2
	mulpd	S, %xmm3

	addpd	%xmm1, %xmm0
	subpd	%xmm3, %xmm2

	movapd	%xmm0, 4 * SIZE(X)
	movlpd	%xmm2, 4 * SIZE(Y)
	movhpd	%xmm2, 5 * SIZE(Y)

	movapd	7 * SIZE(Y), %xmm1
	movapd	6 * SIZE(X), %xmm0

	SHUFPD_1 %xmm1, %xmm4
	movapd	%xmm4, %xmm2
	movapd	%xmm0, %xmm3

	mulpd	C, %xmm0
	mulpd	S, %xmm4

	mulpd	C, %xmm2
	mulpd	S, %xmm3

	addpd	%xmm4, %xmm0
	subpd	%xmm3, %xmm2

	movapd	%xmm0, 6 * SIZE(X)
	movlpd	%xmm2, 6 * SIZE(Y)
	movhpd	%xmm2, 7 * SIZE(Y)

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	movapd	9 * SIZE(Y), %xmm4
	movapd	8 * SIZE(X), %xmm0

	SHUFPD_1 %xmm4, %xmm1
	movapd	%xmm1, %xmm2
	movapd	%xmm0, %xmm3

	mulpd	C, %xmm0
	mulpd	S, %xmm1

	mulpd	C, %xmm2
	mulpd	S, %xmm3

	addpd	%xmm1, %xmm0
	subpd	%xmm3, %xmm2

	movapd	%xmm0, 8 * SIZE(X)
	movlpd	%xmm2, 8 * SIZE(Y)
	movhpd	%xmm2, 9 * SIZE(Y)

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE + 64) - PREOFFSET(X)
#endif

	movapd	11 * SIZE(Y), %xmm1
	movapd	10 * SIZE(X), %xmm0

	SHUFPD_1 %xmm1, %xmm4
	movapd	%xmm4, %xmm2
	movapd	%xmm0, %xmm3

	mulpd	C, %xmm0
	mulpd	S, %xmm4

	mulpd	C, %xmm2
	mulpd	S, %xmm3

	addpd	%xmm4, %xmm0
	subpd	%xmm3, %xmm2

	movapd	%xmm0, 10 * SIZE(X)
	movlpd	%xmm2, 10 * SIZE(Y)
	movhpd	%xmm2, 11 * SIZE(Y)

	movapd	13 * SIZE(Y), %xmm4
	movapd	12 * SIZE(X), %xmm0

	SHUFPD_1 %xmm4, %xmm1
	movapd	%xmm1, %xmm2
	movapd	%xmm0, %xmm3

	mulpd	C, %xmm0
	mulpd	S, %xmm1

	mulpd	C, %xmm2
	mulpd	S, %xmm3

	addpd	%xmm1, %xmm0
	subpd	%xmm3, %xmm2

	movapd	%xmm0, 12 * SIZE(X)
	movlpd	%xmm2, 12 * SIZE(Y)
	movhpd	%xmm2, 13 * SIZE(Y)

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE + 64) - PREOFFSET(Y)
#endif

	movapd	15 * SIZE(Y), %xmm1
	movapd	14 * SIZE(X), %xmm0

	SHUFPD_1 %xmm1, %xmm4
	movapd	%xmm4, %xmm2
	movapd	%xmm0, %xmm3

	mulpd	C, %xmm0
	mulpd	S, %xmm4

	mulpd	C, %xmm2
	mulpd	S, %xmm3

	addpd	%xmm4, %xmm0
	subpd	%xmm3, %xmm2

	movapd	%xmm0, 14 * SIZE(X)
	movlpd	%xmm2, 14 * SIZE(Y)
	movhpd	%xmm2, 15 * SIZE(Y)

	addl	$16 * SIZE, X
	addl	$16 * SIZE, Y
	decl	I
	jg	.L21
	ALIGN_3

.L24:
	testl	$15, N
	jle	.L999

	testl	$8, N
	jle	.L25

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
	movlpd	%xmm2, 0 * SIZE(Y)
	movhpd	%xmm2, 1 * SIZE(Y)

	movapd	3 * SIZE(Y), %xmm1
	movapd	2 * SIZE(X), %xmm0

	SHUFPD_1 %xmm1, %xmm4
	movapd	%xmm4, %xmm2
	movapd	%xmm0, %xmm3

	mulpd	C, %xmm0
	mulpd	S, %xmm4

	mulpd	C, %xmm2
	mulpd	S, %xmm3

	addpd	%xmm4, %xmm0
	subpd	%xmm3, %xmm2

	movapd	%xmm0, 2 * SIZE(X)
	movlpd	%xmm2, 2 * SIZE(Y)
	movhpd	%xmm2, 3 * SIZE(Y)

	movapd	5 * SIZE(Y), %xmm4
	movapd	4 * SIZE(X), %xmm0

	SHUFPD_1 %xmm4, %xmm1
	movapd	%xmm1, %xmm2
	movapd	%xmm0, %xmm3

	mulpd	C, %xmm0
	mulpd	S, %xmm1

	mulpd	C, %xmm2
	mulpd	S, %xmm3

	addpd	%xmm1, %xmm0
	subpd	%xmm3, %xmm2

	movapd	%xmm0, 4 * SIZE(X)
	movlpd	%xmm2, 4 * SIZE(Y)
	movhpd	%xmm2, 5 * SIZE(Y)

	movapd	7 * SIZE(Y), %xmm1
	movapd	6 * SIZE(X), %xmm0

	SHUFPD_1 %xmm1, %xmm4
	movapd	%xmm4, %xmm2
	movapd	%xmm0, %xmm3

	mulpd	C, %xmm0
	mulpd	S, %xmm4

	mulpd	C, %xmm2
	mulpd	S, %xmm3

	addpd	%xmm4, %xmm0
	subpd	%xmm3, %xmm2

	movapd	%xmm0, 6 * SIZE(X)
	movlpd	%xmm2, 6 * SIZE(Y)
	movhpd	%xmm2, 7 * SIZE(Y)

	addl	$8 * SIZE, X
	addl	$8 * SIZE, Y
	ALIGN_3

.L25:
	testl	$4, N
	jle	.L26

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
	movlpd	%xmm2, 0 * SIZE(Y)
	movhpd	%xmm2, 1 * SIZE(Y)

	movapd	3 * SIZE(Y), %xmm1
	movapd	2 * SIZE(X), %xmm0

	SHUFPD_1 %xmm1, %xmm4
	movapd	%xmm4, %xmm2
	movapd	%xmm0, %xmm3

	mulpd	C, %xmm0
	mulpd	S, %xmm4

	mulpd	C, %xmm2
	mulpd	S, %xmm3

	addpd	%xmm4, %xmm0
	subpd	%xmm3, %xmm2

	movapd	%xmm0, 2 * SIZE(X)
	movlpd	%xmm2, 2 * SIZE(Y)
	movhpd	%xmm2, 3 * SIZE(Y)

	addl	$4 * SIZE, X
	addl	$4 * SIZE, Y
	ALIGN_3

.L26:
	testl	$2, N
	jle	.L27

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
	movlpd	%xmm2, 0 * SIZE(Y)
	movhpd	%xmm2, 1 * SIZE(Y)
	movapd	%xmm4, %xmm1

	addl	$2 * SIZE, X
	addl	$2 * SIZE, Y
	ALIGN_3

.L27:
	testl	$1, N
	jle	.L999

	unpckhpd %xmm1, %xmm1
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
	movl	N,  I
	cmpl	$0, INCX
	je  .L56
	cmpl	$0, INCY
	je  .L56
	sarl	$2, I
	jle	.L55
	ALIGN_3

.L53:
	movsd	(Y), %xmm1
	movhpd	(Y, INCY), %xmm1
	movsd	(X), %xmm0
	movhpd	(X, INCX), %xmm0

	movapd	%xmm1, %xmm2
	movapd	%xmm0, %xmm3

	mulpd	C, %xmm0
	mulpd	S, %xmm1

	mulpd	C, %xmm2
	mulpd	S, %xmm3

	addpd	%xmm1, %xmm0
	subpd	%xmm3, %xmm2

	movlpd	%xmm0, (X)
	movhpd	%xmm0, (X, INCX)
	movlpd	%xmm2, (Y)
	movhpd	%xmm2, (Y, INCY)

	leal	(X, INCX, 2), X
	leal	(Y, INCY, 2), Y

	movsd	(Y), %xmm1
	movhpd	(Y, INCY), %xmm1
	movsd	(X), %xmm0
	movhpd	(X, INCX), %xmm0

	movapd	%xmm1, %xmm2
	movapd	%xmm0, %xmm3

	mulpd	C, %xmm0
	mulpd	S, %xmm1

	mulpd	C, %xmm2
	mulpd	S, %xmm3

	addpd	%xmm1, %xmm0
	subpd	%xmm3, %xmm2

	movlpd	%xmm0, (X)
	movhpd	%xmm0, (X, INCX)
	movlpd	%xmm2, (Y)
	movhpd	%xmm2, (Y, INCY)

	leal	(X, INCX, 2), X
	leal	(Y, INCY, 2), Y

	decl	I
	jg	.L53
	ALIGN_3

.L55:
	movl	N,  I
	andl	$3, I
	jle	.L999
	ALIGN_3

.L56:
	movsd	(Y), %xmm1
	movsd	(X), %xmm0

	movapd	%xmm1, %xmm2
	movapd	%xmm0, %xmm3

	mulsd	C, %xmm0
	mulsd	S, %xmm1

	mulsd	C, %xmm2
	mulsd	S, %xmm3

	addsd	%xmm1, %xmm0
	subsd	%xmm3, %xmm2

	movsd	%xmm0, (X)
	movsd	%xmm2, (Y)

	addl	INCX, X
	addl	INCY, Y

	decl	I
	jg	.L56
	ALIGN_3

.L999:
	popl	%ebx
	popl	%esi
	popl	%edi

	ret

	EPILOGUE
