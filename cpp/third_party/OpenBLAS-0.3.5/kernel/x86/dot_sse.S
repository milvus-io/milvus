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

#define N	%ecx
#define X	%esi
#define INCX	%ebx
#define Y	%edi
#define INCY	%edx

#include "l1param.h"

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

#ifdef F_INTERFACE
	movl	(N),   N		# N
	movl	(INCX),INCX		# INCX
	movl	(INCY),INCY		# INCY
#endif

	leal	(, INCX, SIZE), INCX
	leal	(, INCY, SIZE), INCY

	xorps	%xmm0, %xmm0
	xorps	%xmm1, %xmm1
	xorps	%xmm2, %xmm2
	xorps	%xmm3, %xmm3

	cmpl	$0, N
	jle	.L999

	cmpl	$SIZE, INCX
	jne	.L50
	cmpl	$SIZE, INCY
	jne	.L50

	subl	$-32 * SIZE, X
	subl	$-32 * SIZE, Y

	cmpl	$3, N
	jle	.L17

	testl	$SIZE, Y
	je	.L05

	movss	-32 * SIZE(X), %xmm0
	mulss	-32 * SIZE(Y), %xmm0
	addl	$1 * SIZE, X
	addl	$1 * SIZE, Y
	decl	N
	ALIGN_2

.L05:
	testl	$2 * SIZE, Y
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
	addl	$2 * SIZE, X
	addl	$2 * SIZE, Y
	subl	$2, N
	jle	.L999
	ALIGN_2

.L10:
#ifdef ALIGNED_ACCESS
	testl	$2 * SIZE, X
	jne	.L30

	testl	$SIZE, X
	jne	.L20
#else
	testl	$3 * SIZE, X
	jne	.L20
#endif

	movl	N,  %eax
	sarl	$5, %eax
	jle	.L14

	movaps	-32 * SIZE(X), %xmm4
	movaps	-28 * SIZE(X), %xmm5
	movaps	-24 * SIZE(X), %xmm6
	movaps	-20 * SIZE(X), %xmm7

	decl	%eax
	jle	.L12

	ALIGN_3

.L11:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	mulps	-32 * SIZE(Y), %xmm4
	addps	%xmm4,  %xmm0
	movaps	-16 * SIZE(X), %xmm4

	mulps	-28 * SIZE(Y), %xmm5
	addps	%xmm5,  %xmm1
	movaps	-12 * SIZE(X), %xmm5

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	mulps	-24 * SIZE(Y), %xmm6
	addps	%xmm6,  %xmm2
	movaps	 -8 * SIZE(X), %xmm6

	mulps	-20 * SIZE(Y), %xmm7
	addps	%xmm7,  %xmm3
	movaps	 -4 * SIZE(X), %xmm7

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	mulps	-16 * SIZE(Y), %xmm4
	addps	%xmm4,  %xmm0
	movaps	  0 * SIZE(X), %xmm4

	mulps	-12 * SIZE(Y), %xmm5
	addps	%xmm5,  %xmm1
	movaps	  4 * SIZE(X), %xmm5

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	mulps	 -8 * SIZE(Y), %xmm6
	addps	%xmm6,  %xmm2
	movaps	  8 * SIZE(X), %xmm6

	mulps	 -4 * SIZE(Y), %xmm7
	addps	%xmm7,  %xmm3
	movaps	 12 * SIZE(X), %xmm7

	subl	$-32 * SIZE, X
	subl	$-32 * SIZE, Y

	decl	%eax
	jg	.L11
	ALIGN_3

.L12:
	mulps	-32 * SIZE(Y), %xmm4
	addps	%xmm4,  %xmm0
	movaps	-16 * SIZE(X), %xmm4

	mulps	-28 * SIZE(Y), %xmm5
	addps	%xmm5,  %xmm1
	movaps	-12 * SIZE(X), %xmm5

	mulps	-24 * SIZE(Y), %xmm6
	addps	%xmm6,  %xmm2
	movaps	 -8 * SIZE(X), %xmm6

	mulps	-20 * SIZE(Y), %xmm7
	addps	%xmm7,  %xmm3
	movaps	 -4 * SIZE(X), %xmm7

	mulps	-16 * SIZE(Y), %xmm4
	addps	%xmm4,  %xmm0
	mulps	-12 * SIZE(Y), %xmm5
	addps	%xmm5,  %xmm1
	mulps	 -8 * SIZE(Y), %xmm6
	addps	%xmm6,  %xmm2
	mulps	 -4 * SIZE(Y), %xmm7
	addps	%xmm7,  %xmm3

	subl	$-32 * SIZE, X
	subl	$-32 * SIZE, Y
	ALIGN_3

.L14:
	testl	$31, N
	jle	.L999

	testl	$16, N
	jle	.L15

	movaps	-32 * SIZE(X), %xmm4
	movaps	-28 * SIZE(X), %xmm5

	mulps	-32 * SIZE(Y), %xmm4
	addps	%xmm4,  %xmm0
	mulps	-28 * SIZE(Y), %xmm5
	addps	%xmm5,  %xmm1

	movaps	-24 * SIZE(X), %xmm6
	movaps	-20 * SIZE(X), %xmm7

	mulps	-24 * SIZE(Y), %xmm6
	addps	%xmm6,  %xmm2
	mulps	-20 * SIZE(Y), %xmm7
	addps	%xmm7,  %xmm3

	addl	$16 * SIZE, X
	addl	$16 * SIZE, Y
	ALIGN_3

.L15:
	testl	$8, N
	jle	.L16

	movaps	-32 * SIZE(X), %xmm4
	movaps	-28 * SIZE(X), %xmm5

	mulps	-32 * SIZE(Y), %xmm4
	addps	%xmm4,  %xmm0
	mulps	-28 * SIZE(Y), %xmm5
	addps	%xmm5,  %xmm1

	addl	$8 * SIZE, X
	addl	$8 * SIZE, Y
	ALIGN_3

.L16:
	testl	$4, N
	jle	.L17

	movaps	-32 * SIZE(X), %xmm4
	mulps	-32 * SIZE(Y), %xmm4

	addps	%xmm4,  %xmm2

	addl	$4 * SIZE, X
	addl	$4 * SIZE, Y
	ALIGN_3

.L17:
	testl	$2, N
	jle	.L18

#ifdef movsd
	xorps	%xmm4, %xmm4
#endif
	movsd	-32 * SIZE(X), %xmm4
#ifdef movsd
	xorps	%xmm6, %xmm6
#endif
	movsd	-32 * SIZE(Y), %xmm6

	mulps	%xmm6,  %xmm4
	addps	%xmm4,  %xmm3

	addl	$2 * SIZE, X
	addl	$2 * SIZE, Y
	ALIGN_3

.L18:
	testl	$1, N
	jle	.L999

	movss	-32 * SIZE(X), %xmm4
	mulss	-32 * SIZE(Y), %xmm4
	addss	%xmm4,  %xmm0
	jmp	.L999
	ALIGN_3

#ifdef ALIGNED_ACCESS
.L20:

	movaps	-33 * SIZE(X), %xmm4
	addl	$3 * SIZE, X

	movl	N,  %eax
	sarl	$5, %eax
	jle	.L24

	movaps	-32 * SIZE(X), %xmm5
	movaps	-28 * SIZE(X), %xmm6
	movaps	-24 * SIZE(X), %xmm7

	decl	%eax
	jle	.L22
	ALIGN_3

.L21:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movss	%xmm5, %xmm4
	PSHUFD1($0x39, %xmm4)
	mulps	-32 * SIZE(Y), %xmm4
	addps	%xmm4,  %xmm0
	movaps	-20 * SIZE(X), %xmm4

	movss	%xmm6, %xmm5
	PSHUFD1($0x39, %xmm5)
	mulps	-28 * SIZE(Y), %xmm5
	addps	%xmm5,  %xmm1
	movaps	-16 * SIZE(X), %xmm5

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	movss	%xmm7, %xmm6
	PSHUFD1($0x39, %xmm6)
	mulps	-24 * SIZE(Y), %xmm6
	addps	%xmm6,  %xmm2
	movaps	-12 * SIZE(X), %xmm6

	movss	%xmm4, %xmm7
	PSHUFD1($0x39, %xmm7)
	mulps	-20 * SIZE(Y), %xmm7
	addps	%xmm7,  %xmm3
	movaps	 -8 * SIZE(X), %xmm7

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	movss	%xmm5, %xmm4
	PSHUFD1($0x39, %xmm4)
	mulps	-16 * SIZE(Y), %xmm4
	addps	%xmm4,  %xmm0
	movaps	 -4 * SIZE(X), %xmm4

	movss	%xmm6, %xmm5
	PSHUFD1($0x39, %xmm5)
	mulps	-12 * SIZE(Y), %xmm5
	addps	%xmm5,  %xmm1
	movaps	  0 * SIZE(X), %xmm5

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	movss	%xmm7, %xmm6
	PSHUFD1($0x39, %xmm6)
	mulps	 -8 * SIZE(Y), %xmm6
	addps	%xmm6,  %xmm2
	movaps	  4 * SIZE(X), %xmm6

	movss	%xmm4, %xmm7
	PSHUFD1($0x39, %xmm7)
	mulps	 -4 * SIZE(Y), %xmm7
	addps	%xmm7,  %xmm3
	movaps	  8 * SIZE(X), %xmm7

	subl	$-32 * SIZE, X
	subl	$-32 * SIZE, Y

	decl	%eax
	jg	.L21
	ALIGN_3

.L22:
	movss	%xmm5, %xmm4
	PSHUFD1($0x39, %xmm4)
	mulps	-32 * SIZE(Y), %xmm4
	addps	%xmm4,  %xmm0
	movaps	-20 * SIZE(X), %xmm4

	movss	%xmm6, %xmm5
	PSHUFD1($0x39, %xmm5)
	mulps	-28 * SIZE(Y), %xmm5
	addps	%xmm5,  %xmm1
	movaps	-16 * SIZE(X), %xmm5

	movss	%xmm7, %xmm6
	PSHUFD1($0x39, %xmm6)
	mulps	-24 * SIZE(Y), %xmm6
	addps	%xmm6,  %xmm2
	movaps	-12 * SIZE(X), %xmm6

	movss	%xmm4, %xmm7
	PSHUFD1($0x39, %xmm7)
	mulps	-20 * SIZE(Y), %xmm7
	addps	%xmm7,  %xmm3
	movaps	 -8 * SIZE(X), %xmm7

	movss	%xmm5, %xmm4
	PSHUFD1($0x39, %xmm4)
	mulps	-16 * SIZE(Y), %xmm4
	addps	%xmm4,  %xmm0
	movaps	 -4 * SIZE(X), %xmm4

	movss	%xmm6, %xmm5
	PSHUFD1($0x39, %xmm5)
	mulps	-12 * SIZE(Y), %xmm5
	addps	%xmm5,  %xmm1

	movss	%xmm7, %xmm6
	PSHUFD1($0x39, %xmm6)
	mulps	 -8 * SIZE(Y), %xmm6
	addps	%xmm6,  %xmm2

	movss	%xmm4, %xmm7
	PSHUFD1($0x39, %xmm7)
	mulps	 -4 * SIZE(Y), %xmm7
	addps	%xmm7,  %xmm3

	subl	$-32 * SIZE, X
	subl	$-32 * SIZE, Y
	ALIGN_3

.L24:
	testl	$31, N
	jle	.L999

	testl	$16, N
	jle	.L25

	movaps	-32 * SIZE(X), %xmm5
	movaps	-28 * SIZE(X), %xmm6
	movaps	-24 * SIZE(X), %xmm7

	movss	%xmm5, %xmm4
	PSHUFD1($0x39, %xmm4)
	mulps	-32 * SIZE(Y), %xmm4
	addps	%xmm4,  %xmm0
	movaps	-20 * SIZE(X), %xmm4

	movss	%xmm6, %xmm5
	PSHUFD1($0x39, %xmm5)
	mulps	-28 * SIZE(Y), %xmm5
	addps	%xmm5,  %xmm1

	movss	%xmm7, %xmm6
	PSHUFD1($0x39, %xmm6)
	mulps	-24 * SIZE(Y), %xmm6
	addps	%xmm6,  %xmm2

	movss	%xmm4, %xmm7
	PSHUFD1($0x39, %xmm7)
	mulps	-20 * SIZE(Y), %xmm7
	addps	%xmm7,  %xmm3

	addl	$16 * SIZE, X
	addl	$16 * SIZE, Y
	ALIGN_3

.L25:
	testl	$8, N
	jle	.L26

	movaps	-32 * SIZE(X), %xmm5
	movaps	-28 * SIZE(X), %xmm6

	movss	%xmm5, %xmm4
	PSHUFD1($0x39, %xmm4)
	mulps	-32 * SIZE(Y), %xmm4
	addps	%xmm4, %xmm0

	movss	%xmm6, %xmm5
	PSHUFD1($0x39, %xmm5)
	mulps	-28 * SIZE(Y), %xmm5
	addps	%xmm5, %xmm1

	movaps	%xmm6, %xmm4

	addl	$8 * SIZE, X
	addl	$8 * SIZE, Y
	ALIGN_3

.L26:
	testl	$4, N
	jle	.L27

	movaps	-32 * SIZE(X), %xmm5
	movss	%xmm5, %xmm4
	PSHUFD1($0x39, %xmm4)
	mulps	-32 * SIZE(Y), %xmm4
	addps	%xmm4, %xmm2
	movaps	%xmm5, %xmm4

	addl	$4 * SIZE, X
	addl	$4 * SIZE, Y
	ALIGN_3

.L27:
	testl	$2, N
	jle	.L28

#ifdef movsd
	xorps	%xmm6, %xmm6
#endif
	movsd	-32 * SIZE(Y), %xmm6

	PSHUFD2($0x39, %xmm4, %xmm5)

	mulps	%xmm6,  %xmm5
	addps	%xmm5,  %xmm3
	movhlps	%xmm4,  %xmm4

	addl	$2 * SIZE, X
	addl	$2 * SIZE, Y
	ALIGN_3

.L28:
	testl	$1, N
	jle	.L999

	PSHUFD1($0x39, %xmm4)
	mulss	-32 * SIZE(Y), %xmm4
	addss	%xmm4,  %xmm0
	jmp	.L999
	ALIGN_3

.L30:
	testl	$SIZE, X
	jne	.L40

	movhps	-32 * SIZE(X), %xmm4
	addl	$2 * SIZE, X

	movl	N,  %eax
	sarl	$5, %eax
	jle	.L34

	movaps	-32 * SIZE(X), %xmm5
	movaps	-28 * SIZE(X), %xmm6
	movaps	-24 * SIZE(X), %xmm7

	decl	%eax
	jle	.L32

	ALIGN_3

.L31:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	SHUFPD_1 %xmm5, %xmm4
	mulps	-32 * SIZE(Y), %xmm4
	addps	 %xmm4,  %xmm0
	movaps	-20 * SIZE(X), %xmm4

	SHUFPD_1 %xmm6, %xmm5
	mulps	-28 * SIZE(Y), %xmm5
	addps	 %xmm5,  %xmm1
	movaps	-16 * SIZE(X), %xmm5

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	SHUFPD_1 %xmm7, %xmm6
	mulps	-24 * SIZE(Y), %xmm6
	addps	 %xmm6,  %xmm2
	movaps	-12 * SIZE(X), %xmm6

	SHUFPD_1 %xmm4, %xmm7
	mulps	-20 * SIZE(Y), %xmm7
	addps	 %xmm7,  %xmm3
	movaps	 -8 * SIZE(X), %xmm7

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	SHUFPD_1 %xmm5, %xmm4
	mulps	-16 * SIZE(Y), %xmm4
	addps	 %xmm4,  %xmm0
	movaps	 -4 * SIZE(X), %xmm4

	SHUFPD_1 %xmm6, %xmm5
	mulps	-12 * SIZE(Y), %xmm5
	addps	 %xmm5,  %xmm1
	movaps	  0 * SIZE(X), %xmm5

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	SHUFPD_1 %xmm7, %xmm6
	mulps	 -8 * SIZE(Y), %xmm6
	addps	 %xmm6, %xmm2
	movaps	  4 * SIZE(X), %xmm6

	SHUFPD_1 %xmm4, %xmm7
	mulps	 -4 * SIZE(Y), %xmm7
	addps	 %xmm7, %xmm3
	movaps	  8 * SIZE(X), %xmm7

	subl	$-32 * SIZE, X
	subl	$-32 * SIZE, Y

	decl	%eax
	jg	.L31
	ALIGN_3

.L32:
	SHUFPD_1 %xmm5, %xmm4
	mulps	-32 * SIZE(Y), %xmm4
	addps	 %xmm4,  %xmm0
	movaps	-20 * SIZE(X), %xmm4

	SHUFPD_1 %xmm6, %xmm5
	mulps	-28 * SIZE(Y), %xmm5
	addps	 %xmm5,  %xmm1
	movaps	-16 * SIZE(X), %xmm5

	SHUFPD_1 %xmm7, %xmm6
	mulps	-24 * SIZE(Y), %xmm6
	addps	 %xmm6,  %xmm2
	movaps	-12 * SIZE(X), %xmm6

	SHUFPD_1 %xmm4, %xmm7
	mulps	-20 * SIZE(Y), %xmm7
	addps	 %xmm7,  %xmm3
	movaps	 -8 * SIZE(X), %xmm7

	SHUFPD_1 %xmm5, %xmm4
	mulps	-16 * SIZE(Y), %xmm4
	addps	 %xmm4,  %xmm0
	movaps	 -4 * SIZE(X), %xmm4

	SHUFPD_1 %xmm6, %xmm5
	mulps	-12 * SIZE(Y), %xmm5
	addps	 %xmm5,  %xmm1

	SHUFPD_1 %xmm7, %xmm6
	mulps	 -8 * SIZE(Y), %xmm6
	addps	 %xmm6, %xmm2

	SHUFPD_1 %xmm4, %xmm7
	mulps	 -4 * SIZE(Y), %xmm7
	addps	 %xmm7, %xmm3

	subl	$-32 * SIZE, X
	subl	$-32 * SIZE, Y
	ALIGN_3

.L34:
	testl	$31, N
	jle	.L999

	testl	$16, N
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

	addl	$16 * SIZE, X
	addl	$16 * SIZE, Y
	ALIGN_3

.L35:
	testl	$8, N
	jle	.L36

	movaps	-32 * SIZE(X), %xmm5
	movaps	-28 * SIZE(X), %xmm6

	SHUFPD_1 %xmm5, %xmm4
	mulps	-32 * SIZE(Y), %xmm4
	addps	 %xmm4, %xmm0
	SHUFPD_1 %xmm6, %xmm5
	mulps	-28 * SIZE(Y), %xmm5
	addps	 %xmm5, %xmm1
	movaps	 %xmm6, %xmm4

	addl	$8 * SIZE, X
	addl	$8 * SIZE, Y
	ALIGN_3

.L36:
	testl	$4, N
	jle	.L37

	movaps	-32 * SIZE(X), %xmm5

	SHUFPD_1 %xmm5, %xmm4
	mulps	-32 * SIZE(Y), %xmm4
	addps	%xmm4, %xmm0
	movaps	%xmm5, %xmm4

	addl	$4 * SIZE, X
	addl	$4 * SIZE, Y
	ALIGN_3

.L37:
	testl	$2, N
	jle	.L38

	xorps	%xmm5, %xmm5
	movhlps	%xmm4, %xmm5

	mulps	-32 * SIZE(Y), %xmm5
	addps	%xmm5, %xmm0

	addl	$2 * SIZE, X
	addl	$2 * SIZE, Y
	ALIGN_3

.L38:
	testl	$1, N
	jle	.L999

	movss	-34 * SIZE(X), %xmm4
	mulss	-32 * SIZE(Y), %xmm4
	addss	%xmm4,  %xmm0
	jmp	.L999
	ALIGN_3

.L40:
	movaps	-35 * SIZE(X), %xmm4
	addl	$SIZE, X

	movl	N,  %eax
	sarl	$5, %eax
	jle	.L44

	movaps	-32 * SIZE(X), %xmm5
	movaps	-28 * SIZE(X), %xmm6
	movaps	-24 * SIZE(X), %xmm7

	decl	%eax
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
	movaps	-20 * SIZE(X), %xmm4

	movss	%xmm6, %xmm5
	shufps	$0x93, %xmm6,  %xmm5
	mulps	-28 * SIZE(Y), %xmm5
	addps	%xmm5,  %xmm1
	movaps	-16 * SIZE(X), %xmm5

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	movss	%xmm7, %xmm6
	shufps	$0x93, %xmm7,  %xmm6
	mulps	-24 * SIZE(Y), %xmm6
	addps	%xmm6,  %xmm2
	movaps	-12 * SIZE(X), %xmm6

	movss	%xmm4, %xmm7
	shufps	$0x93, %xmm4,  %xmm7
	mulps	-20 * SIZE(Y), %xmm7
	addps	%xmm7,  %xmm3
	movaps	 -8 * SIZE(X), %xmm7

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	movss	%xmm5, %xmm4
	shufps	$0x93, %xmm5,  %xmm4
	mulps	-16 * SIZE(Y), %xmm4
	addps	%xmm4,  %xmm0
	movaps	 -4 * SIZE(X), %xmm4

	movss	%xmm6, %xmm5
	shufps	$0x93, %xmm6, %xmm5
	mulps	-12 * SIZE(Y), %xmm5
	addps	%xmm5,  %xmm1
	movaps	  0 * SIZE(X), %xmm5

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	movss	%xmm7, %xmm6
	shufps	$0x93, %xmm7, %xmm6
	mulps	 -8 * SIZE(Y), %xmm6
	addps	%xmm6,  %xmm2
	movaps	  4 * SIZE(X), %xmm6

	movss	%xmm4, %xmm7
	shufps	$0x93, %xmm4,  %xmm7
	mulps	 -4 * SIZE(Y), %xmm7
	addps	%xmm7,  %xmm3
	movaps	  8 * SIZE(X), %xmm7

	subl	$-32 * SIZE, X
	subl	$-32 * SIZE, Y

	decl	%eax
	jg	.L41
	ALIGN_3

.L42:
	movss	%xmm5, %xmm4
	shufps	$0x93, %xmm5,  %xmm4
	mulps	-32 * SIZE(Y), %xmm4
	addps	%xmm4,  %xmm0
	movaps	-20 * SIZE(X), %xmm4

	movss	%xmm6, %xmm5
	shufps	$0x93, %xmm6,  %xmm5
	mulps	-28 * SIZE(Y), %xmm5
	addps	%xmm5,  %xmm1
	movaps	-16 * SIZE(X), %xmm5

	movss	%xmm7, %xmm6
	shufps	$0x93, %xmm7,  %xmm6
	mulps	-24 * SIZE(Y), %xmm6
	addps	%xmm6,  %xmm2
	movaps	-12 * SIZE(X), %xmm6

	movss	%xmm4, %xmm7
	shufps	$0x93, %xmm4,  %xmm7
	mulps	-20 * SIZE(Y), %xmm7
	addps	%xmm7,  %xmm3
	movaps	 -8 * SIZE(X), %xmm7

	movss	%xmm5, %xmm4
	shufps	$0x93, %xmm5,  %xmm4
	mulps	-16 * SIZE(Y), %xmm4
	addps	%xmm4,  %xmm0
	movaps	 -4 * SIZE(X), %xmm4

	movss	%xmm6, %xmm5
	shufps	$0x93, %xmm6, %xmm5
	mulps	-12 * SIZE(Y), %xmm5
	addps	%xmm5,  %xmm1

	movss	%xmm7, %xmm6
	shufps	$0x93, %xmm7, %xmm6
	mulps	 -8 * SIZE(Y), %xmm6
	addps	%xmm6,  %xmm2

	movss	%xmm4, %xmm7
	shufps	$0x93, %xmm4,  %xmm7
	mulps	 -4 * SIZE(Y), %xmm7
	addps	%xmm7,  %xmm3

	subl	$-32 * SIZE, X
	subl	$-32 * SIZE, Y
	ALIGN_3

.L44:
	testl	$31, N
	jle	.L999

	testl	$16, N
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

	addl	$16 * SIZE, X
	addl	$16 * SIZE, Y
	ALIGN_3

.L45:
	testl	$8, N
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

	addl	$8 * SIZE, X
	addl	$8 * SIZE, Y
	ALIGN_3

.L46:
	testl	$4, N
	jle	.L47

	movaps	-32 * SIZE(X), %xmm5
	movss	%xmm5, %xmm4
	shufps	$0x93, %xmm5,  %xmm4
	mulps	-32 * SIZE(Y), %xmm4
	addps	%xmm4, %xmm2
	movaps	%xmm5, %xmm4

	addl	$4 * SIZE, X
	addl	$4 * SIZE, Y
	ALIGN_3

.L47:
	testl	$2, N
	jle	.L48

	movaps	-32 * SIZE(X), %xmm5
#ifdef movsd
	xorps	%xmm7, %xmm7
#endif
	movsd	-32 * SIZE(Y), %xmm7

	movss	%xmm5, %xmm4
	shufps	$0x93, %xmm5,  %xmm4

	mulps	%xmm7,  %xmm4
	addps	%xmm4,  %xmm3
	movlhps	%xmm5,  %xmm4

	addl	$2 * SIZE, X
	addl	$2 * SIZE, Y
	ALIGN_3

.L48:
	testl	$1, N
	jle	.L999

	PSHUFD1($0x93, %xmm4)
	mulss	-32 * SIZE(Y), %xmm4
	addss	%xmm4,  %xmm0
	jmp	.L999
	ALIGN_4

#else
.L20:
	movl	N,  %eax
	sarl	$5, %eax
	jle	.L24

	movlps	-32 * SIZE(X), %xmm4
	movhps	-30 * SIZE(X), %xmm4
	movlps	-28 * SIZE(X), %xmm5
	movhps	-26 * SIZE(X), %xmm5
	movlps	-24 * SIZE(X), %xmm6
	movhps	-22 * SIZE(X), %xmm6
	movlps	-20 * SIZE(X), %xmm7
	movhps	-18 * SIZE(X), %xmm7

	decl	%eax
	jle	.L22

	ALIGN_3

.L21:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	mulps	-32 * SIZE(Y), %xmm4
	addps	%xmm4,  %xmm0
	movlps	-16 * SIZE(X), %xmm4
	movhps	-14 * SIZE(X), %xmm4

	mulps	-28 * SIZE(Y), %xmm5
	addps	%xmm5,  %xmm1
	movlps	-12 * SIZE(X), %xmm5
	movhps	-10 * SIZE(X), %xmm5

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	mulps	-24 * SIZE(Y), %xmm6
	addps	%xmm6,  %xmm2
	movlps	 -8 * SIZE(X), %xmm6
	movhps	 -6 * SIZE(X), %xmm6

	mulps	-20 * SIZE(Y), %xmm7
	addps	%xmm7,  %xmm3
	movlps	 -4 * SIZE(X), %xmm7
	movhps	 -2 * SIZE(X), %xmm7

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	mulps	-16 * SIZE(Y), %xmm4
	addps	%xmm4,  %xmm0
	movlps	  0 * SIZE(X), %xmm4
	movhps	  2 * SIZE(X), %xmm4

	mulps	-12 * SIZE(Y), %xmm5
	addps	%xmm5,  %xmm1
	movlps	  4 * SIZE(X), %xmm5
	movhps	  6 * SIZE(X), %xmm5

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	mulps	 -8 * SIZE(Y), %xmm6
	addps	%xmm6,  %xmm2
	movlps	  8 * SIZE(X), %xmm6
	movhps	 10 * SIZE(X), %xmm6

	mulps	 -4 * SIZE(Y), %xmm7
	addps	%xmm7,  %xmm3
	movlps	 12 * SIZE(X), %xmm7
	movhps	 14 * SIZE(X), %xmm7

	subl	$-32 * SIZE, X
	subl	$-32 * SIZE, Y

	decl	%eax
	jg	.L21
	ALIGN_3

.L22:
	mulps	-32 * SIZE(Y), %xmm4
	addps	%xmm4,  %xmm0
	movlps	-16 * SIZE(X), %xmm4
	movhps	-14 * SIZE(X), %xmm4

	mulps	-28 * SIZE(Y), %xmm5
	addps	%xmm5,  %xmm1
	movlps	-12 * SIZE(X), %xmm5
	movhps	-10 * SIZE(X), %xmm5

	mulps	-24 * SIZE(Y), %xmm6
	addps	%xmm6,  %xmm2
	movlps	 -8 * SIZE(X), %xmm6
	movhps	 -6 * SIZE(X), %xmm6

	mulps	-20 * SIZE(Y), %xmm7
	addps	%xmm7,  %xmm3
	movlps	 -4 * SIZE(X), %xmm7
	movhps	 -2 * SIZE(X), %xmm7

	mulps	-16 * SIZE(Y), %xmm4
	addps	%xmm4,  %xmm0
	mulps	-12 * SIZE(Y), %xmm5
	addps	%xmm5,  %xmm1
	mulps	 -8 * SIZE(Y), %xmm6
	addps	%xmm6,  %xmm2
	mulps	 -4 * SIZE(Y), %xmm7
	addps	%xmm7,  %xmm3

	subl	$-32 * SIZE, X
	subl	$-32 * SIZE, Y
	ALIGN_3

.L24:
	testl	$31, N
	jle	.L999

	testl	$16, N
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

	addl	$16 * SIZE, X
	addl	$16 * SIZE, Y
	ALIGN_3

.L25:
	testl	$8, N
	jle	.L26

	movlps	-32 * SIZE(X), %xmm4
	movhps	-30 * SIZE(X), %xmm4
	movlps	-28 * SIZE(X), %xmm5
	movhps	-26 * SIZE(X), %xmm5

	mulps	-32 * SIZE(Y), %xmm4
	addps	%xmm4,  %xmm0
	mulps	-28 * SIZE(Y), %xmm5
	addps	%xmm5,  %xmm1

	addl	$8 * SIZE, X
	addl	$8 * SIZE, Y
	ALIGN_3

.L26:
	testl	$4, N
	jle	.L27

	movlps	-32 * SIZE(X), %xmm4
	movhps	-30 * SIZE(X), %xmm4
	mulps	-32 * SIZE(Y), %xmm4

	addps	%xmm4,  %xmm2

	addl	$4 * SIZE, X
	addl	$4 * SIZE, Y
	ALIGN_3

.L27:
	testl	$2, N
	jle	.L28

#ifdef movsd
	xorps	%xmm4, %xmm4
#endif
	movsd	-32 * SIZE(X), %xmm4
#ifdef movsd
	xorps	%xmm6, %xmm6
#endif
	movsd	-32 * SIZE(Y), %xmm6

	mulps	%xmm6,  %xmm4
	addps	%xmm4,  %xmm3

	addl	$2 * SIZE, X
	addl	$2 * SIZE, Y
	ALIGN_3

.L28:
	testl	$1, N
	jle	.L999

	movss	-32 * SIZE(X), %xmm4
	mulss	-32 * SIZE(Y), %xmm4
	addss	%xmm4,  %xmm0
	jmp	.L999
	ALIGN_3
#endif

.L50:
	movl	N,  %eax
	sarl	$2, %eax
	jle	.L55
	ALIGN_3

.L53:
	movss	0 * SIZE(X), %xmm4
	addl	INCX, X
	mulss	0 * SIZE(Y), %xmm4
	addl	INCY, Y
	movss	0 * SIZE(X), %xmm5
	addl	INCX, X
	mulss	0 * SIZE(Y), %xmm5
	addl	INCY, Y
	movss	0 * SIZE(X), %xmm6
	addl	INCX, X
	mulss	0 * SIZE(Y), %xmm6
	addl	INCY, Y
	movss	0 * SIZE(X), %xmm7
	addl	INCX, X
	mulss	0 * SIZE(Y), %xmm7
	addl	INCY, Y

	addss	%xmm4, %xmm0
	addss	%xmm5, %xmm1
	addss	%xmm6, %xmm2
	addss	%xmm7, %xmm3

	decl	%eax
	jg	.L53
	ALIGN_3

.L55:
	movl	N, %eax
	andl	$3,   %eax
	jle	.L999
	ALIGN_3

.L56:
	movss	0 * SIZE(X), %xmm4
	addl	INCX, X
	mulss	0 * SIZE(Y), %xmm4
	addl	INCY, Y
	addss	%xmm4, %xmm0
	decl	%eax
	jg	.L56
	ALIGN_3

.L999:
	addps	%xmm1, %xmm0
	addps	%xmm3, %xmm2
	addps	%xmm2, %xmm0

#if defined(HAVE_SSE3) && !defined(__INTERIX)
	haddps	%xmm0, %xmm0
	haddps	%xmm0, %xmm0
#elif defined(HAVE_SSE2)
	movhlps	%xmm0, %xmm1
	addps	%xmm1, %xmm0

	PSHUFD2($1, %xmm0, %xmm1)
	addss	 %xmm1, %xmm0
#else
	movhlps	%xmm0, %xmm1
	addps	%xmm1, %xmm0

	movaps	%xmm0, %xmm1
	shufps	$1, %xmm0, %xmm0
	addss	 %xmm1, %xmm0
#endif

	movss	%xmm0, STACK_N
	flds	STACK_N

	popl	%ebx
	popl	%esi
	popl	%edi
	ret

	EPILOGUE
