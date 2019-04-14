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
#define Y	ARG4	/* rcx */
#ifndef WINDOWS_ABI
#define INCY	ARG5	/* r8  */
#else
#define INCY	%r10
#endif

#include "l1param.h"

#ifdef OPTERON
#define LOAD(OFFSET, ADDR, REG)		xorps	REG, REG; addps	OFFSET(ADDR), REG
#else
#define LOAD(OFFSET, ADDR, REG)		movaps	OFFSET(ADDR), REG
#endif

	PROLOGUE
	PROFCODE

#ifdef WINDOWS_ABI
	movq	40(%rsp), INCY
#endif

	SAVEREGISTERS

	salq	$ZBASE_SHIFT, INCX
	salq	$ZBASE_SHIFT, INCY

	cmpq	$2 * SIZE, INCX
	jne	.L100
	cmpq	$2 * SIZE, INCY
	jne	.L100

	cmpq	$3, M
	jle	.L106

	subq	$-32 * SIZE, X
	subq	$-32 * SIZE, Y
	addq	M, M

	testq	$SIZE, Y
	je	.L05

	movss	-32 * SIZE(X), %xmm0
	movss	%xmm0, -32 * SIZE(Y)
	addq	$1 * SIZE, X
	addq	$1 * SIZE, Y
	decq	M
	ALIGN_4

.L05:
	testq	$2 * SIZE, Y
	je	.L10

	movsd	-32 * SIZE(X), %xmm0
	movlps	%xmm0, -32 * SIZE(Y)
	addq	$2 * SIZE, X
	addq	$2 * SIZE, Y
	subq	$2, M
	jle	.L19
	ALIGN_4

.L10:
	testq	$3 * SIZE, X
	jne	.L20

	movq	M,  %rax
	sarq	$5, %rax
	jle	.L13

	movaps	-32 * SIZE(X), %xmm0
	movaps	-28 * SIZE(X), %xmm1
	movaps	-24 * SIZE(X), %xmm2
	movaps	-20 * SIZE(X), %xmm3
	movaps	-16 * SIZE(X), %xmm4
	movaps	-12 * SIZE(X), %xmm5
	movaps	 -8 * SIZE(X), %xmm6
	movaps	 -4 * SIZE(X), %xmm7

	decq	%rax
	jle .L12
	ALIGN_3

.L11:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	movaps	%xmm0, -32 * SIZE(Y)
	LOAD( 0 * SIZE, X, %xmm0)
	movaps	%xmm1, -28 * SIZE(Y)
	LOAD( 4 * SIZE, X, %xmm1)

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movaps	%xmm2, -24 * SIZE(Y)
	LOAD( 8 * SIZE, X, %xmm2)
	movaps	%xmm3, -20 * SIZE(Y)
	LOAD(12 * SIZE, X, %xmm3)

#if defined(PREFETCHW) && !defined(FETCH128)
	PREFETCHW (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	movaps	%xmm4,-16 * SIZE(Y)
	LOAD(16 * SIZE, X, %xmm4)
	movaps	%xmm5,-12 * SIZE(Y)
	LOAD(20 * SIZE, X, %xmm5)

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	movaps	%xmm6, -8 * SIZE(Y)
	LOAD(24 * SIZE, X, %xmm6)
	movaps	%xmm7, -4 * SIZE(Y)
	LOAD(28 * SIZE, X, %xmm7)

	subq	$-32 * SIZE, Y
	subq	$-32 * SIZE, X
	decq	%rax
	jg	.L11
	ALIGN_3

.L12:
	movaps	%xmm0, -32 * SIZE(Y)
	movaps	%xmm1, -28 * SIZE(Y)
	movaps	%xmm2, -24 * SIZE(Y)
	movaps	%xmm3, -20 * SIZE(Y)
	movaps	%xmm4, -16 * SIZE(Y)
	movaps	%xmm5, -12 * SIZE(Y)
	movaps	%xmm6,  -8 * SIZE(Y)
	movaps	%xmm7,  -4 * SIZE(Y)

	subq	$-32 * SIZE, Y
	subq	$-32 * SIZE, X
	ALIGN_3

.L13:
	testq	$16, M
	jle	.L14

	movaps	-32 * SIZE(X), %xmm0
	movaps	-28 * SIZE(X), %xmm1
	movaps	-24 * SIZE(X), %xmm2
	movaps	-20 * SIZE(X), %xmm3

	movaps	%xmm0, -32 * SIZE(Y)
	movaps	%xmm1, -28 * SIZE(Y)
	movaps	%xmm2, -24 * SIZE(Y)
	movaps	%xmm3, -20 * SIZE(Y)

	addq	$16 * SIZE, X
	addq	$16 * SIZE, Y
	ALIGN_3

.L14:
	testq	$8, M
	jle	.L15

	movaps	-32 * SIZE(X), %xmm0
	movaps	-28 * SIZE(X), %xmm1

	movaps	%xmm0, -32 * SIZE(Y)
	movaps	%xmm1, -28 * SIZE(Y)

	addq	$8 * SIZE, X
	addq	$8 * SIZE, Y
	ALIGN_3

.L15:
	testq	$4, M
	jle	.L16

	movaps	-32 * SIZE(X), %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	addq	$4 * SIZE, X
	addq	$4 * SIZE, Y
	ALIGN_3

.L16:
	testq	$2, M
	jle	.L17

	movsd	-32 * SIZE(X), %xmm0
	movlps	%xmm0, -32 * SIZE(Y)

	addq	$2 * SIZE, X
	addq	$2 * SIZE, Y
	ALIGN_3

.L17:
	testq	$1, M
	jle	.L19

	movss	-32 * SIZE(X), %xmm0
	movss	%xmm0, 	-32 * SIZE(Y)
	ALIGN_3

.L19:
	xorq	%rax,%rax

	RESTOREREGISTERS

	ret
	ALIGN_3


.L20:
	testq	$SIZE, X
	jne	.L30

	movhps	-32 * SIZE(X), %xmm0

	movq	M,  %rax
	sarq	$5, %rax
	jle	.L23

	movaps	-30 * SIZE(X), %xmm1
	movaps	-26 * SIZE(X), %xmm2
	movaps	-22 * SIZE(X), %xmm3
	movaps	-18 * SIZE(X), %xmm4
	movaps	-14 * SIZE(X), %xmm5
	movaps	-10 * SIZE(X), %xmm6
	movaps	 -6 * SIZE(X), %xmm7

	decq	%rax
	jle .L22
	ALIGN_4

.L21:

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	shufps	$0x4e, %xmm1, %xmm0
	movaps	%xmm0, -32 * SIZE(Y)
	movaps	 -2 * SIZE(X), %xmm0

	shufps	$0x4e, %xmm2, %xmm1
	movaps	%xmm1, -28 * SIZE(Y)
	movaps	  2 * SIZE(X), %xmm1

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	shufps	$0x4e, %xmm3, %xmm2
	movaps	%xmm2, -24 * SIZE(Y)
	movaps	  6 * SIZE(X), %xmm2

	shufps	$0x4e, %xmm4, %xmm3
	movaps	%xmm3, -20 * SIZE(Y)
	movaps	 10 * SIZE(X), %xmm3

#if defined(PREFETCHW) && !defined(FETCH128)
	PREFETCHW (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	shufps	$0x4e, %xmm5, %xmm4
	movaps	%xmm4, -16 * SIZE(Y)
	movaps	 14 * SIZE(X), %xmm4

	shufps	$0x4e, %xmm6, %xmm5
	movaps	%xmm5, -12 * SIZE(Y)
	movaps	 18 * SIZE(X), %xmm5

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	shufps	$0x4e, %xmm7, %xmm6
	movaps	%xmm6,  -8 * SIZE(Y)
	movaps	 22 * SIZE(X), %xmm6

	shufps	$0x4e, %xmm0, %xmm7
	movaps	%xmm7,  -4 * SIZE(Y)
	movaps	 26 * SIZE(X), %xmm7

	subq	$-32 * SIZE, X
	subq	$-32 * SIZE, Y
	decq	%rax
	jg	.L21
	ALIGN_3

.L22:
	shufps	$0x4e, %xmm1, %xmm0
	movaps	%xmm0, -32 * SIZE(Y)
	movaps	 -2 * SIZE(X), %xmm0

	shufps	$0x4e, %xmm2, %xmm1
	movaps	%xmm1, -28 * SIZE(Y)

	shufps	$0x4e, %xmm3, %xmm2
	movaps	%xmm2, -24 * SIZE(Y)

	shufps	$0x4e, %xmm4, %xmm3
	movaps	%xmm3, -20 * SIZE(Y)

	shufps	$0x4e, %xmm5, %xmm4
	movaps	%xmm4, -16 * SIZE(Y)

	shufps	$0x4e, %xmm6, %xmm5
	movaps	%xmm5, -12 * SIZE(Y)

	shufps	$0x4e, %xmm7, %xmm6
	movaps	%xmm6,  -8 * SIZE(Y)

	shufps	$0x4e, %xmm0, %xmm7
	movaps	%xmm7,  -4 * SIZE(Y)

	subq	$-32 * SIZE, X
	subq	$-32 * SIZE, Y
	ALIGN_3

.L23:
	testq	$16, M
	jle	.L24
	ALIGN_3

	movaps	-30 * SIZE(X), %xmm1
	movaps	-26 * SIZE(X), %xmm2
	movaps	-22 * SIZE(X), %xmm3
	movaps	-18 * SIZE(X), %xmm4

	shufps	$0x4e, %xmm1, %xmm0
	movaps	%xmm0, -32 * SIZE(Y)
	shufps	$0x4e, %xmm2, %xmm1
	movaps	%xmm1, -28 * SIZE(Y)
	shufps	$0x4e, %xmm3, %xmm2
	movaps	%xmm2, -24 * SIZE(Y)
	shufps	$0x4e, %xmm4, %xmm3
	movaps	%xmm3, -20 * SIZE(Y)

	movaps	%xmm4, %xmm0

	addq	$16 * SIZE, X
	addq	$16 * SIZE, Y
	ALIGN_3

.L24:
	testq	$8, M
	jle	.L25
	ALIGN_3

	movaps	-30 * SIZE(X), %xmm1
	movaps	-26 * SIZE(X), %xmm2

	shufps	$0x4e, %xmm1, %xmm0
	shufps	$0x4e, %xmm2, %xmm1

	movaps	%xmm0, -32 * SIZE(Y)
	movaps	%xmm1, -28 * SIZE(Y)
	movaps	%xmm2, %xmm0

	addq	$8 * SIZE, X
	addq	$8 * SIZE, Y
	ALIGN_3

.L25:
	testq	$4, M
	jle	.L26
	ALIGN_3

	movaps	-30 * SIZE(X), %xmm1
	shufps	$0x4e, %xmm1, %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	addq	$4 * SIZE, X
	addq	$4 * SIZE, Y
	ALIGN_3

.L26:
	testq	$2, M
	jle	.L27
	ALIGN_3

	movsd	-32 * SIZE(X), %xmm0

	movsd	%xmm0, -32 * SIZE(Y)

	addq	$2 * SIZE, X
	addq	$2 * SIZE, Y
	ALIGN_3

.L27:
	testq	$1, M
	jle	.L29
	ALIGN_3

	movss	-32 * SIZE(X), %xmm0
	movss	%xmm0, 	-32 * SIZE(Y)
	addq	$SIZE, Y
	ALIGN_3

.L29:
	xorq	%rax,%rax

	RESTOREREGISTERS

	ret
	ALIGN_3

.L30:
	testq	$2 * SIZE, X
	jne	.L40

	movaps	-33 * SIZE(X), %xmm0

	movq	M,  %rax
	sarq	$5, %rax
	jle	.L33

	movaps	-29 * SIZE(X), %xmm1
	movaps	-25 * SIZE(X), %xmm2
	movaps	-21 * SIZE(X), %xmm3
	movaps	-17 * SIZE(X), %xmm4
	movaps	-13 * SIZE(X), %xmm5
	movaps	 -9 * SIZE(X), %xmm6
	movaps	 -5 * SIZE(X), %xmm7

	decq	%rax
	jle .L32
	ALIGN_4

.L31:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	movss	%xmm1, %xmm0
	shufps	$0x39, %xmm0, %xmm0
	movaps	%xmm0, -32 * SIZE(Y)
	movaps	 -1 * SIZE(X), %xmm0

	movss	%xmm2, %xmm1
	shufps	$0x39, %xmm1, %xmm1
	movaps	%xmm1, -28 * SIZE(Y)
	movaps	  3 * SIZE(X), %xmm1

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movss	%xmm3, %xmm2
	shufps	$0x39, %xmm2, %xmm2
	movaps	%xmm2, -24 * SIZE(Y)
	movaps	  7 * SIZE(X), %xmm2

	movss	%xmm4, %xmm3
	shufps	$0x39, %xmm3, %xmm3
	movaps	%xmm3, -20 * SIZE(Y)
	movaps	 11 * SIZE(X), %xmm3

#if defined(PREFETCHW) && !defined(FETCH128)
	PREFETCHW (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	movss	%xmm5, %xmm4
	shufps	$0x39, %xmm4, %xmm4
	movaps	%xmm4, -16 * SIZE(Y)
	movaps	 15 * SIZE(X), %xmm4

	movss	%xmm6, %xmm5
	shufps	$0x39, %xmm5, %xmm5
	movaps	%xmm5, -12 * SIZE(Y)
	movaps	 19 * SIZE(X), %xmm5

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	movss	%xmm7, %xmm6
	shufps	$0x39, %xmm6, %xmm6
	movaps	%xmm6,  -8 * SIZE(Y)
	movaps	 23 * SIZE(X), %xmm6

	movss	%xmm0, %xmm7
	shufps	$0x39, %xmm7, %xmm7
	movaps	%xmm7,  -4 * SIZE(Y)
	movaps	 27 * SIZE(X), %xmm7

	subq	$-32 * SIZE, X
	subq	$-32 * SIZE, Y
	decq	%rax
	jg	.L31
	ALIGN_3

.L32:
	movss	%xmm1, %xmm0
	shufps	$0x39, %xmm0, %xmm0
	movaps	%xmm0, -32 * SIZE(Y)
	movaps	 -1 * SIZE(X), %xmm0

	movss	%xmm2, %xmm1
	shufps	$0x39, %xmm1, %xmm1
	movaps	%xmm1, -28 * SIZE(Y)

	movss	%xmm3, %xmm2
	shufps	$0x39, %xmm2, %xmm2
	movaps	%xmm2, -24 * SIZE(Y)

	movss	%xmm4, %xmm3
	shufps	$0x39, %xmm3, %xmm3
	movaps	%xmm3, -20 * SIZE(Y)

	movss	%xmm5, %xmm4
	shufps	$0x39, %xmm4, %xmm4
	movaps	%xmm4, -16 * SIZE(Y)

	movss	%xmm6, %xmm5
	shufps	$0x39, %xmm5, %xmm5
	movaps	%xmm5, -12 * SIZE(Y)

	movss	%xmm7, %xmm6
	shufps	$0x39, %xmm6, %xmm6
	movaps	%xmm6,  -8 * SIZE(Y)

	movss	%xmm0, %xmm7
	shufps	$0x39, %xmm7, %xmm7
	movaps	%xmm7,  -4 * SIZE(Y)

	subq	$-32 * SIZE, X
	subq	$-32 * SIZE, Y
	ALIGN_3

.L33:
	testq	$16, M
	jle	.L34
	ALIGN_3

	movaps	-29 * SIZE(X), %xmm1
	movaps	-25 * SIZE(X), %xmm2
	movaps	-21 * SIZE(X), %xmm3
	movaps	-17 * SIZE(X), %xmm4

	movss	%xmm1, %xmm0
	shufps	$0x39, %xmm0, %xmm0
 	movaps	%xmm0, -32 * SIZE(Y)

	movss	%xmm2, %xmm1
	shufps	$0x39, %xmm1, %xmm1
	movaps	%xmm1, -28 * SIZE(Y)

	movss	%xmm3, %xmm2
	shufps	$0x39, %xmm2, %xmm2
 	movaps	%xmm2, -24 * SIZE(Y)

	movss	%xmm4, %xmm3
	shufps	$0x39, %xmm3, %xmm3
	movaps	%xmm3, -20 * SIZE(Y)

	movaps	%xmm4, %xmm0

	addq	$16 * SIZE, X
	addq	$16 * SIZE, Y
	ALIGN_3

.L34:
	testq	$8, M
	jle	.L35
	ALIGN_3

	movaps	-29 * SIZE(X), %xmm1
	movaps	-25 * SIZE(X), %xmm2

	movss	%xmm1, %xmm0
	shufps	$0x39, %xmm0, %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	movss	%xmm2, %xmm1
	shufps	$0x39, %xmm1, %xmm1
	movaps	%xmm1, -28 * SIZE(Y)
	movaps	%xmm2, %xmm0

	addq	$8 * SIZE, X
	addq	$8 * SIZE, Y
	ALIGN_3

.L35:
	testq	$4, M
	jle	.L36
	ALIGN_3

	movaps	-29 * SIZE(X), %xmm1

	movss	%xmm1, %xmm0
	shufps	$0x39, %xmm0, %xmm0

	movaps	%xmm0, -32 * SIZE(Y)

	addq	$4 * SIZE, X
	addq	$4 * SIZE, Y
	ALIGN_3

.L36:
	testq	$2, M
	jle	.L37
	ALIGN_3

	movsd	-32 * SIZE(X), %xmm0
	movsd	%xmm0, -32 * SIZE(Y)

	addq	$2 * SIZE, X
	addq	$2 * SIZE, Y
	ALIGN_3

.L37:
	testq	$1, M
	jle	.L39
	ALIGN_3

	movss	-32 * SIZE(X), %xmm0
	movss	%xmm0, 	-32 * SIZE(Y)
	addq	$SIZE, Y
	ALIGN_3

.L39:
	xorq	%rax,%rax

	RESTOREREGISTERS

	ret
	ALIGN_3

.L40:
	movaps	-35 * SIZE(X), %xmm0

	movq	M,  %rax
	sarq	$5, %rax
	jle	.L43

	movaps	-31 * SIZE(X), %xmm1
	movaps	-27 * SIZE(X), %xmm2
	movaps	-23 * SIZE(X), %xmm3
	movaps	-19 * SIZE(X), %xmm4
	movaps	-15 * SIZE(X), %xmm5
	movaps	-11 * SIZE(X), %xmm6
	movaps	 -7 * SIZE(X), %xmm7

	decq	%rax
	jle .L42
	ALIGN_4

.L41:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	movss	%xmm1, %xmm0
	shufps	$0x93, %xmm1, %xmm0
	movaps	%xmm0, -32 * SIZE(Y)
	movaps	 -3 * SIZE(X), %xmm0

	movss	%xmm2, %xmm1
	shufps	$0x93, %xmm2, %xmm1
	movaps	%xmm1, -28 * SIZE(Y)
	movaps	  1 * SIZE(X), %xmm1

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movss	%xmm3, %xmm2
	shufps	$0x93, %xmm3, %xmm2
	movaps	%xmm2, -24 * SIZE(Y)
	movaps	  5 * SIZE(X), %xmm2

	movss	%xmm4, %xmm3
	shufps	$0x93, %xmm4, %xmm3
	movaps	%xmm3, -20 * SIZE(Y)
	movaps	  9 * SIZE(X), %xmm3

#if defined(PREFETCHW) && !defined(FETCH128)
	PREFETCHW (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	movss	%xmm5, %xmm4
	shufps	$0x93, %xmm5, %xmm4
	movaps	%xmm4, -16 * SIZE(Y)
	movaps	 13 * SIZE(X), %xmm4

	movss	%xmm6, %xmm5
	shufps	$0x93, %xmm6, %xmm5
	movaps	%xmm5, -12 * SIZE(Y)
	movaps	 17 * SIZE(X), %xmm5

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	movss	%xmm7, %xmm6
	shufps	$0x93, %xmm7, %xmm6
	movaps	%xmm6, -8 * SIZE(Y)
	movaps	 21 * SIZE(X), %xmm6

	movss	%xmm0, %xmm7
	shufps	$0x93, %xmm0, %xmm7
	movaps	%xmm7, -4 * SIZE(Y)
	movaps	 25 * SIZE(X), %xmm7

	subq	$-32 * SIZE, X
	subq	$-32 * SIZE, Y
	decq	%rax
	jg	.L41
	ALIGN_3

.L42:
	movss	%xmm1, %xmm0
	shufps	$0x93, %xmm1, %xmm0
	movaps	%xmm0, -32 * SIZE(Y)
	movaps	 -3 * SIZE(X), %xmm0

	movss	%xmm2, %xmm1
	shufps	$0x93, %xmm2, %xmm1
	movaps	%xmm1, -28 * SIZE(Y)

	movss	%xmm3, %xmm2
	shufps	$0x93, %xmm3, %xmm2
	movaps	%xmm2, -24 * SIZE(Y)

	movss	%xmm4, %xmm3
	shufps	$0x93, %xmm4, %xmm3
	movaps	%xmm3, -20 * SIZE(Y)

	movss	%xmm5, %xmm4
	shufps	$0x93, %xmm5, %xmm4
	movaps	%xmm4, -16 * SIZE(Y)

	movss	%xmm6, %xmm5
	shufps	$0x93, %xmm6, %xmm5
	movaps	%xmm5, -12 * SIZE(Y)

	movss	%xmm7, %xmm6
	shufps	$0x93, %xmm7, %xmm6
	movaps	%xmm6, -8 * SIZE(Y)

	movss	%xmm0, %xmm7
	shufps	$0x93, %xmm0, %xmm7
	movaps	%xmm7, -4 * SIZE(Y)

	subq	$-32 * SIZE, X
	subq	$-32 * SIZE, Y
	ALIGN_3

.L43:
	testq	$16, M
	jle	.L44
	ALIGN_3

	movaps	-31 * SIZE(X), %xmm1
	movaps	-27 * SIZE(X), %xmm2
	movaps	-23 * SIZE(X), %xmm3
	movaps	-19 * SIZE(X), %xmm4

	movss	%xmm1, %xmm0
	shufps	$0x93, %xmm1, %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	movss	%xmm2, %xmm1
	shufps	$0x93, %xmm2, %xmm1
	movaps	%xmm1, -28 * SIZE(Y)

	movss	%xmm3, %xmm2
	shufps	$0x93, %xmm3, %xmm2
	movaps	%xmm2, -24 * SIZE(Y)

	movss	%xmm4, %xmm3
	shufps	$0x93, %xmm4, %xmm3
	movaps	%xmm3, -20 * SIZE(Y)

	movaps	%xmm4, %xmm0

	addq	$16 * SIZE, X
	addq	$16 * SIZE, Y
	ALIGN_3

.L44:
	testq	$8, M
	jle	.L45
	ALIGN_3

	movaps	-31 * SIZE(X), %xmm1
	movaps	-27 * SIZE(X), %xmm2

	movss	%xmm1, %xmm0
	shufps	$0x93, %xmm1, %xmm0
	movaps	%xmm0, -32 * SIZE(Y)

	movss	%xmm2, %xmm1
	shufps	$0x93, %xmm2, %xmm1
	movaps	%xmm1, -28 * SIZE(Y)

	movaps	%xmm2, %xmm0

	addq	$8 * SIZE, X
	addq	$8 * SIZE, Y
	ALIGN_3

.L45:
	testq	$4, M
	jle	.L46
	ALIGN_3

	movaps	-31 * SIZE(X), %xmm1

	movss	%xmm1, %xmm0
	shufps	$0x93, %xmm1, %xmm0

	movaps	%xmm0, -32 * SIZE(Y)

	addq	$4 * SIZE, X
	addq	$4 * SIZE, Y
	ALIGN_3

.L46:
	testq	$2, M
	jle	.L47
	ALIGN_3

	movsd	-32 * SIZE(X), %xmm0
	movsd	%xmm0, -32 * SIZE(Y)

	addq	$2 * SIZE, X
	addq	$2 * SIZE, Y
	ALIGN_3

.L47:
	testq	$1, M
	jle	.L49
	ALIGN_3

	movss	-32 * SIZE(X), %xmm0
	movss	%xmm0, 	-32 * SIZE(Y)
	addq	$SIZE, Y
	ALIGN_3

.L49:
	xorq	%rax,%rax

	RESTOREREGISTERS

	ret
	ALIGN_4

.L100:
	movq	M,  %rax
	sarq	$3, %rax
	jle	.L105
	ALIGN_3

.L102:
	movsd	 (X), %xmm0
	addq	 INCX, X
	movhps	 (X), %xmm0
	addq	 INCX, X
	movsd	 (X), %xmm1
	addq	 INCX, X
	movhps	 (X), %xmm1
	addq	 INCX, X
	movsd	 (X), %xmm2
	addq	 INCX, X
	movhps	 (X), %xmm2
	addq	 INCX, X
	movsd	 (X), %xmm3
	addq	 INCX, X
	movhps	 (X), %xmm3
	addq	 INCX, X

	movsd	%xmm0,  (Y)
	addq	 INCY, Y
	movhps	%xmm0,  (Y)
	addq	 INCY, Y
	movsd	%xmm1,  (Y)
	addq	 INCY, Y
	movhps	%xmm1,  (Y)
	addq	 INCY, Y
	movsd	%xmm2,  (Y)
	addq	 INCY, Y
	movhps	%xmm2,  (Y)
	addq	 INCY, Y
	movsd	%xmm3,  (Y)
	addq	 INCY, Y
	movhps	%xmm3,  (Y)
	addq	 INCY, Y

	decq	%rax
	jg	.L102
	ALIGN_3

.L105:
	testq	$4, M
	jle	.L106

	movsd	 (X), %xmm0
	addq	 INCX, X
	movhps	 (X), %xmm0
	addq	 INCX, X
	movsd	 (X), %xmm1
	addq	 INCX, X
	movhps	 (X), %xmm1
	addq	 INCX, X

	movsd	%xmm0,  (Y)
	addq	 INCY, Y
	movhps	%xmm0,  (Y)
	addq	 INCY, Y
	movsd	%xmm1,  (Y)
	addq	 INCY, Y
	movhps	%xmm1,  (Y)
	addq	 INCY, Y
	ALIGN_3

.L106:
	testq	$2, M
	jle	.L107

	movsd	 (X), %xmm0
	addq	 INCX, X
	movhps	 (X), %xmm0
	addq	 INCX, X

	movsd	%xmm0,  (Y)
	addq	 INCY, Y
	movhps	%xmm0,  (Y)
	addq	 INCY, Y
	ALIGN_3

.L107:
	testq	$1, M
	jle	.L999

	movsd	 (X), %xmm0
	movsd	%xmm0,   (Y)
	ALIGN_3

.L999:
	xorq	%rax, %rax

	RESTOREREGISTERS

	ret

	EPILOGUE

