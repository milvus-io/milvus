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

#define N	%ebx
#define X	%esi
#define INCX	%ecx
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

	sall	$ZBASE_SHIFT, INCX
	sall	$ZBASE_SHIFT, INCY

	xorps	%xmm0, %xmm0
	xorps	%xmm1, %xmm1

	testl	N, N
	jle	.L999

	cmpl	$2 * SIZE, INCX
	jne	.L200
	cmpl	$2 * SIZE, INCY
	jne	.L200

	subl	$-32 * SIZE, X
	subl	$-32 * SIZE, Y

	testl	$SIZE, X
	jne	.L50

.L0x:
	testl	$2 * SIZE, X
	je	.L10

#ifdef movsd
	xorps	%xmm4, %xmm4
#endif
	movsd	-32 * SIZE(X), %xmm4
#ifdef movsd
	xorps	%xmm6, %xmm6
#endif
	movsd	-32 * SIZE(Y), %xmm0

	PSHUFD2($0xb1, %xmm0, %xmm1)
	mulps	%xmm4, %xmm0
	mulps	%xmm4, %xmm1
	addl	$2 * SIZE, X
	addl	$2 * SIZE, Y
	decl	N
	ALIGN_3

.L10:
	testl	$3 * SIZE, Y
	jne	.L20

	movl	N,  %eax
	sarl	$4, %eax
	jle	.L15

	movaps	-32 * SIZE(X), %xmm4
	movaps	-32 * SIZE(Y), %xmm6
	movaps	-28 * SIZE(X), %xmm5
	movaps	-28 * SIZE(Y), %xmm7

	decl	%eax
	jle	.L12
	ALIGN_3

.L11:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	PSHUFD2($0xb1, %xmm6, %xmm3)
	mulps	%xmm4, %xmm6
	addps	%xmm6, %xmm0
	movaps	-24 * SIZE(Y), %xmm6
	mulps	%xmm4, %xmm3
	movaps	-24 * SIZE(X), %xmm4
	addps	%xmm3, %xmm1

	PSHUFD2($0xb1, %xmm7, %xmm3)
	mulps	%xmm5, %xmm7
	addps	%xmm7, %xmm0
	movaps	-20 * SIZE(Y), %xmm7
	mulps	%xmm5, %xmm3
	movaps	-20 * SIZE(X), %xmm5
	addps	%xmm3, %xmm1

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	PSHUFD2($0xb1, %xmm6, %xmm3)
	mulps	%xmm4, %xmm6
	addps	%xmm6, %xmm0
	movaps	-16 * SIZE(Y), %xmm6
	mulps	%xmm4, %xmm3
	movaps	-16 * SIZE(X), %xmm4
	addps	%xmm3, %xmm1

	PSHUFD2($0xb1, %xmm7, %xmm3)
	mulps	%xmm5, %xmm7
	addps	%xmm7, %xmm0
	movaps	-12 * SIZE(Y), %xmm7
	mulps	%xmm5, %xmm3
	movaps	-12 * SIZE(X), %xmm5
	addps	%xmm3, %xmm1

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	PSHUFD2($0xb1, %xmm6, %xmm3)
	mulps	%xmm4, %xmm6
	addps	%xmm6, %xmm0
	movaps	 -8 * SIZE(Y), %xmm6
	mulps	%xmm4, %xmm3
	movaps	 -8 * SIZE(X), %xmm4
	addps	%xmm3, %xmm1

	PSHUFD2($0xb1, %xmm7, %xmm3)
	mulps	%xmm5, %xmm7
	addps	%xmm7, %xmm0
	movaps	 -4 * SIZE(Y), %xmm7
	mulps	%xmm5, %xmm3
	movaps	 -4 * SIZE(X), %xmm5
	addps	%xmm3, %xmm1

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	PSHUFD2($0xb1, %xmm6, %xmm3)
	mulps	%xmm4, %xmm6
	addps	%xmm6, %xmm0
	movaps	  0 * SIZE(Y), %xmm6
	mulps	%xmm4, %xmm3
	movaps	  0 * SIZE(X), %xmm4
	addps	%xmm3, %xmm1

	PSHUFD2($0xb1, %xmm7, %xmm3)
	mulps	%xmm5, %xmm7
	addps	%xmm7, %xmm0
	movaps	  4 * SIZE(Y), %xmm7
	mulps	%xmm5, %xmm3
	movaps	  4 * SIZE(X), %xmm5
	addps	%xmm3, %xmm1

	subl	$-32 * SIZE, X
	subl	$-32 * SIZE, Y

	decl	%eax
	jg	.L11
	ALIGN_3

.L12:
	PSHUFD2($0xb1, %xmm6, %xmm3)
	mulps	%xmm4, %xmm6
	addps	%xmm6, %xmm0
	movaps	-24 * SIZE(Y), %xmm6
	mulps	%xmm4, %xmm3
	movaps	-24 * SIZE(X), %xmm4
	addps	%xmm3, %xmm1

	PSHUFD2($0xb1, %xmm7, %xmm3)
	mulps	%xmm5, %xmm7
	addps	%xmm7, %xmm0
	movaps	-20 * SIZE(Y), %xmm7
	mulps	%xmm5, %xmm3
	movaps	-20 * SIZE(X), %xmm5
	addps	%xmm3, %xmm1

	PSHUFD2($0xb1, %xmm6, %xmm3)
	mulps	%xmm4, %xmm6
	addps	%xmm6, %xmm0
	movaps	-16 * SIZE(Y), %xmm6
	mulps	%xmm4, %xmm3
	movaps	-16 * SIZE(X), %xmm4
	addps	%xmm3, %xmm1

	PSHUFD2($0xb1, %xmm7, %xmm3)
	mulps	%xmm5, %xmm7
	addps	%xmm7, %xmm0
	movaps	-12 * SIZE(Y), %xmm7
	mulps	%xmm5, %xmm3
	movaps	-12 * SIZE(X), %xmm5
	addps	%xmm3, %xmm1

	PSHUFD2($0xb1, %xmm6, %xmm3)
	mulps	%xmm4, %xmm6
	addps	%xmm6, %xmm0
	movaps	 -8 * SIZE(Y), %xmm6
	mulps	%xmm4, %xmm3
	movaps	 -8 * SIZE(X), %xmm4
	addps	%xmm3, %xmm1

	PSHUFD2($0xb1, %xmm7, %xmm3)
	mulps	%xmm5, %xmm7
	addps	%xmm7, %xmm0
	movaps	 -4 * SIZE(Y), %xmm7
	mulps	%xmm5, %xmm3
	movaps	 -4 * SIZE(X), %xmm5
	addps	%xmm3, %xmm1

	PSHUFD2($0xb1, %xmm6, %xmm3)
	mulps	%xmm4, %xmm6
	addps	%xmm6, %xmm0
	mulps	%xmm4, %xmm3
	addps	%xmm3, %xmm1

	PSHUFD2($0xb1, %xmm7, %xmm3)
	mulps	%xmm5, %xmm7
	addps	%xmm7, %xmm0
	mulps	%xmm5, %xmm3
	addps	%xmm3, %xmm1

	subl	$-32 * SIZE, X
	subl	$-32 * SIZE, Y
	ALIGN_3

.L15:
	testl	$8, N
	jle	.L16

	movaps	-32 * SIZE(X), %xmm4
	movaps	-32 * SIZE(Y), %xmm6
	movaps	-28 * SIZE(X), %xmm5
	movaps	-28 * SIZE(Y), %xmm7

	PSHUFD2($0xb1, %xmm6, %xmm3)
	mulps	%xmm4, %xmm6
	addps	%xmm6, %xmm0
	movaps	-24 * SIZE(Y), %xmm6
	mulps	%xmm4, %xmm3
	movaps	-24 * SIZE(X), %xmm4
	addps	%xmm3, %xmm1

	PSHUFD2($0xb1, %xmm7, %xmm3)
	mulps	%xmm5, %xmm7
	addps	%xmm7, %xmm0
	movaps	-20 * SIZE(Y), %xmm7
	mulps	%xmm5, %xmm3
	movaps	-20 * SIZE(X), %xmm5
	addps	%xmm3, %xmm1

	PSHUFD2($0xb1, %xmm6, %xmm3)
	mulps	%xmm4, %xmm6
	addps	%xmm6, %xmm0
	mulps	%xmm4, %xmm3
	addps	%xmm3, %xmm1

	PSHUFD2($0xb1, %xmm7, %xmm3)
	mulps	%xmm5, %xmm7
	addps	%xmm7, %xmm0
	mulps	%xmm5, %xmm3
	addps	%xmm3, %xmm1

	addl	$16 * SIZE, X
	addl	$16 * SIZE, Y
	ALIGN_3

.L16:
	testl	$4, N
	jle	.L17

	movaps	-32 * SIZE(X), %xmm4
	movaps	-32 * SIZE(Y), %xmm6
	movaps	-28 * SIZE(X), %xmm5
	movaps	-28 * SIZE(Y), %xmm7

	PSHUFD2($0xb1, %xmm6, %xmm3)
	mulps	%xmm4, %xmm6
	addps	%xmm6, %xmm0
	mulps	%xmm4, %xmm3
	addps	%xmm3, %xmm1

	PSHUFD2($0xb1, %xmm7, %xmm3)
	mulps	%xmm5, %xmm7
	addps	%xmm7, %xmm0
	mulps	%xmm5, %xmm3
	addps	%xmm3, %xmm1

	addl	$8 * SIZE, X
	addl	$8 * SIZE, Y
	ALIGN_3

.L17:
	testl	$2, N
	jle	.L18

	movaps	-32 * SIZE(X), %xmm4
	movaps	-32 * SIZE(Y), %xmm6

	PSHUFD2($0xb1, %xmm6, %xmm3)
	mulps	%xmm4, %xmm6
	addps	%xmm6, %xmm0
	mulps	%xmm4, %xmm3
	addps	%xmm3, %xmm1

	addl	$4 * SIZE, X
	addl	$4 * SIZE, Y
	ALIGN_3

.L18:
	testl	$1, N
	jle	.L98

#ifdef movsd
	xorps	%xmm4, %xmm4
#endif
	movsd	-32 * SIZE(X), %xmm4
#ifdef movsd
	xorps	%xmm6, %xmm6
#endif
	movsd	-32 * SIZE(Y), %xmm6

	PSHUFD2($0xb1, %xmm6, %xmm3)
	mulps	%xmm4, %xmm6
	addps	%xmm6, %xmm0
	mulps	%xmm4, %xmm3
	addps	%xmm3, %xmm1
	jmp	.L98
	ALIGN_3

.L20:
#ifdef ALIGNED_ACCESS

	testl	$2 * SIZE, Y
	jne	.L30

	movaps	-33 * SIZE(Y), %xmm6
	addl	$3 * SIZE, Y

	shufps	$0xb1, %xmm1, %xmm1

	movl	N,  %eax
	sarl	$4, %eax
	jle	.L25

	movaps	-32 * SIZE(X), %xmm4
	movaps	-28 * SIZE(X), %xmm5
	movaps	-32 * SIZE(Y), %xmm7

	decl	%eax
	jle	.L22
	ALIGN_3

.L21:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	movss	%xmm7, %xmm6
	PSHUFD2($0xb1, %xmm4, %xmm3)
	shufps	$0x39, %xmm6, %xmm6
	mulps	%xmm6, %xmm4
	addps	%xmm4, %xmm0
	movaps	-24 * SIZE(X), %xmm4
	mulps	%xmm6, %xmm3
	movaps	-28 * SIZE(Y), %xmm6
	addps	%xmm3, %xmm1

	movss	%xmm6, %xmm7
	PSHUFD2($0xb1, %xmm5, %xmm3)
	shufps	$0x39, %xmm7, %xmm7
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm0
	movaps	-20 * SIZE(X), %xmm5
	mulps	%xmm7, %xmm3
	movaps	-24 * SIZE(Y), %xmm7
	addps	%xmm3, %xmm1

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movss	%xmm7, %xmm6
	PSHUFD2($0xb1, %xmm4, %xmm3)
	shufps	$0x39, %xmm6, %xmm6
	mulps	%xmm6, %xmm4
	addps	%xmm4, %xmm0
	movaps	-16 * SIZE(X), %xmm4
	mulps	%xmm6, %xmm3
	movaps	-20 * SIZE(Y), %xmm6
	addps	%xmm3, %xmm1

	movss	%xmm6, %xmm7
	PSHUFD2($0xb1, %xmm5, %xmm3)
	shufps	$0x39, %xmm7, %xmm7
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm0
	movaps	-12 * SIZE(X), %xmm5
	mulps	%xmm7, %xmm3
	movaps	-16 * SIZE(Y), %xmm7
	addps	%xmm3, %xmm1

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	movss	%xmm7, %xmm6
	PSHUFD2($0xb1, %xmm4, %xmm3)
	shufps	$0x39, %xmm6, %xmm6
	mulps	%xmm6, %xmm4
	addps	%xmm4, %xmm0
	movaps	 -8 * SIZE(X), %xmm4
	mulps	%xmm6, %xmm3
	movaps	-12 * SIZE(Y), %xmm6
	addps	%xmm3, %xmm1

	movss	%xmm6, %xmm7
	PSHUFD2($0xb1, %xmm5, %xmm3)
	shufps	$0x39, %xmm7, %xmm7
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm0
	movaps	 -4 * SIZE(X), %xmm5
	mulps	%xmm7, %xmm3
	movaps	 -8 * SIZE(Y), %xmm7
	addps	%xmm3, %xmm1

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	movss	%xmm7, %xmm6
	PSHUFD2($0xb1, %xmm4, %xmm3)
	shufps	$0x39, %xmm6, %xmm6
	mulps	%xmm6, %xmm4
	addps	%xmm4, %xmm0
	movaps	  0 * SIZE(X), %xmm4
	mulps	%xmm6, %xmm3
	movaps	 -4 * SIZE(Y), %xmm6
	addps	%xmm3, %xmm1

	movss	%xmm6, %xmm7
	PSHUFD2($0xb1, %xmm5, %xmm3)
	shufps	$0x39, %xmm7, %xmm7
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm0
	movaps	  4 * SIZE(X), %xmm5
	mulps	%xmm7, %xmm3
	movaps	  0 * SIZE(Y), %xmm7
	addps	%xmm3, %xmm1

	subl	$-32 * SIZE, X
	subl	$-32 * SIZE, Y

	decl	%eax
	jg	.L21
	ALIGN_3

.L22:
	movss	%xmm7, %xmm6
	PSHUFD2($0xb1, %xmm4, %xmm3)
	shufps	$0x39, %xmm6, %xmm6
	mulps	%xmm6, %xmm4
	addps	%xmm4, %xmm0
	movaps	-24 * SIZE(X), %xmm4
	mulps	%xmm6, %xmm3
	movaps	-28 * SIZE(Y), %xmm6
	addps	%xmm3, %xmm1

	movss	%xmm6, %xmm7
	PSHUFD2($0xb1, %xmm5, %xmm3)
	shufps	$0x39, %xmm7, %xmm7
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm0
	movaps	-20 * SIZE(X), %xmm5
	mulps	%xmm7, %xmm3
	movaps	-24 * SIZE(Y), %xmm7
	addps	%xmm3, %xmm1

	movss	%xmm7, %xmm6
	PSHUFD2($0xb1, %xmm4, %xmm3)
	shufps	$0x39, %xmm6, %xmm6
	mulps	%xmm6, %xmm4
	addps	%xmm4, %xmm0
	movaps	-16 * SIZE(X), %xmm4
	mulps	%xmm6, %xmm3
	movaps	-20 * SIZE(Y), %xmm6
	addps	%xmm3, %xmm1

	movss	%xmm6, %xmm7
	PSHUFD2($0xb1, %xmm5, %xmm3)
	shufps	$0x39, %xmm7, %xmm7
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm0
	movaps	-12 * SIZE(X), %xmm5
	mulps	%xmm7, %xmm3
	movaps	-16 * SIZE(Y), %xmm7
	addps	%xmm3, %xmm1

	movss	%xmm7, %xmm6
	PSHUFD2($0xb1, %xmm4, %xmm3)
	shufps	$0x39, %xmm6, %xmm6
	mulps	%xmm6, %xmm4
	addps	%xmm4, %xmm0
	movaps	 -8 * SIZE(X), %xmm4
	mulps	%xmm6, %xmm3
	movaps	-12 * SIZE(Y), %xmm6
	addps	%xmm3, %xmm1

	movss	%xmm6, %xmm7
	PSHUFD2($0xb1, %xmm5, %xmm3)
	shufps	$0x39, %xmm7, %xmm7
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm0
	movaps	 -4 * SIZE(X), %xmm5
	mulps	%xmm7, %xmm3
	movaps	 -8 * SIZE(Y), %xmm7
	addps	%xmm3, %xmm1

	movss	%xmm7, %xmm6
	PSHUFD2($0xb1, %xmm4, %xmm3)
	shufps	$0x39, %xmm6, %xmm6
	mulps	%xmm6, %xmm4
	addps	%xmm4, %xmm0
	mulps	%xmm6, %xmm3
	movaps	 -4 * SIZE(Y), %xmm6
	addps	%xmm3, %xmm1

	movss	%xmm6, %xmm7
	PSHUFD2($0xb1, %xmm5, %xmm3)
	shufps	$0x39, %xmm7, %xmm7
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm0
	mulps	%xmm7, %xmm3
	addps	%xmm3, %xmm1

	subl	$-32 * SIZE, X
	subl	$-32 * SIZE, Y
	ALIGN_3

.L25:
	testl	$8, N
	jle	.L26

	movaps	-32 * SIZE(X), %xmm4
	movaps	-28 * SIZE(X), %xmm5
	movaps	-32 * SIZE(Y), %xmm7

	movss	%xmm7, %xmm6
	PSHUFD2($0xb1, %xmm4, %xmm3)
	shufps	$0x39, %xmm6, %xmm6
	mulps	%xmm6, %xmm4
	addps	%xmm4, %xmm0
	movaps	-24 * SIZE(X), %xmm4
	mulps	%xmm6, %xmm3
	movaps	-28 * SIZE(Y), %xmm6
	addps	%xmm3, %xmm1

	movss	%xmm6, %xmm7
	PSHUFD2($0xb1, %xmm5, %xmm3)
	shufps	$0x39, %xmm7, %xmm7
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm0
	movaps	-20 * SIZE(X), %xmm5
	mulps	%xmm7, %xmm3
	movaps	-24 * SIZE(Y), %xmm7
	addps	%xmm3, %xmm1

	movss	%xmm7, %xmm6
	PSHUFD2($0xb1, %xmm4, %xmm3)
	shufps	$0x39, %xmm6, %xmm6
	mulps	%xmm6, %xmm4
	addps	%xmm4, %xmm0
	mulps	%xmm6, %xmm3
	movaps	-20 * SIZE(Y), %xmm6
	addps	%xmm3, %xmm1

	movss	%xmm6, %xmm7
	PSHUFD2($0xb1, %xmm5, %xmm3)
	shufps	$0x39, %xmm7, %xmm7
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm0
	mulps	%xmm7, %xmm3
	addps	%xmm3, %xmm1

	addl	$16 * SIZE, X
	addl	$16 * SIZE, Y
	ALIGN_3

.L26:
	testl	$4, N
	jle	.L27

	movaps	-32 * SIZE(X), %xmm4
	movaps	-28 * SIZE(X), %xmm5
	movaps	-32 * SIZE(Y), %xmm7

	movss	%xmm7, %xmm6
	PSHUFD2($0xb1, %xmm4, %xmm3)
	shufps	$0x39, %xmm6, %xmm6
	mulps	%xmm6, %xmm4
	addps	%xmm4, %xmm0
	mulps	%xmm6, %xmm3
	movaps	-28 * SIZE(Y), %xmm6
	addps	%xmm3, %xmm1

	movss	%xmm6, %xmm7
	PSHUFD2($0xb1, %xmm5, %xmm3)
	shufps	$0x39, %xmm7, %xmm7
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm0
	mulps	%xmm7, %xmm3
	addps	%xmm3, %xmm1

	addl	$8 * SIZE, X
	addl	$8 * SIZE, Y
	ALIGN_3

.L27:
	testl	$2, N
	jle	.L28

	movaps	-32 * SIZE(X), %xmm4
	movaps	-32 * SIZE(Y), %xmm7

	movss	%xmm7, %xmm6
	PSHUFD2($0xb1, %xmm4, %xmm3)
	shufps	$0x39, %xmm6, %xmm6
	mulps	%xmm6, %xmm4
	addps	%xmm4, %xmm0
	mulps	%xmm6, %xmm3
	addps	%xmm3, %xmm1

	movaps	%xmm7, %xmm6
	addl	$4 * SIZE, X
	addl	$4 * SIZE, Y
	ALIGN_3

.L28:
	testl	$1, N
	jle	.L29

#ifdef movsd
	xorps	%xmm4, %xmm4
#endif
	movsd	-32 * SIZE(X), %xmm4

	PSHUFD2($0xb1, %xmm4, %xmm3)
	shufps	$0x39, %xmm6, %xmm6
	mulps	%xmm6, %xmm4
	addps	%xmm4, %xmm0
	mulps	%xmm6, %xmm3
	addps	%xmm3, %xmm1
	ALIGN_3

.L29:
	shufps	$0xb1, %xmm1, %xmm1
	jmp	.L98
	ALIGN_3

.L30:
	testl	$SIZE, Y
	jne	.L40
#endif

	movl	N,  %eax
	sarl	$4, %eax
	jle	.L35

	movaps	-32 * SIZE(X), %xmm4
	movsd	-32 * SIZE(Y), %xmm6
	movhps	-30 * SIZE(Y), %xmm6

	movaps	-28 * SIZE(X), %xmm5
	movsd	-28 * SIZE(Y), %xmm7
	movhps	-26 * SIZE(Y), %xmm7

	decl	%eax
	jle	.L32
	ALIGN_3

.L31:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	PSHUFD2($0xb1, %xmm6, %xmm3)
	mulps	%xmm4, %xmm6
	addps	%xmm6, %xmm0
	movsd	-24 * SIZE(Y), %xmm6
	movhps	-22 * SIZE(Y), %xmm6
	mulps	%xmm4, %xmm3
	movaps	-24 * SIZE(X), %xmm4
	addps	%xmm3, %xmm1

	PSHUFD2($0xb1, %xmm7, %xmm3)
	mulps	%xmm5, %xmm7
	addps	%xmm7, %xmm0
	movsd	-20 * SIZE(Y), %xmm7
	movhps	-18 * SIZE(Y), %xmm7
	mulps	%xmm5, %xmm3
	movaps	-20 * SIZE(X), %xmm5
	addps	%xmm3, %xmm1

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	PSHUFD2($0xb1, %xmm6, %xmm3)
	mulps	%xmm4, %xmm6
	addps	%xmm6, %xmm0
	movsd	-16 * SIZE(Y), %xmm6
	movhps	-14 * SIZE(Y), %xmm6
	mulps	%xmm4, %xmm3
	movaps	-16 * SIZE(X), %xmm4
	addps	%xmm3, %xmm1

	PSHUFD2($0xb1, %xmm7, %xmm3)
	mulps	%xmm5, %xmm7
	addps	%xmm7, %xmm0
	movsd	-12 * SIZE(Y), %xmm7
	movhps	-10 * SIZE(Y), %xmm7
	mulps	%xmm5, %xmm3
	movaps	-12 * SIZE(X), %xmm5
	addps	%xmm3, %xmm1

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	PSHUFD2($0xb1, %xmm6, %xmm3)
	mulps	%xmm4, %xmm6
	addps	%xmm6, %xmm0
	movsd	 -8 * SIZE(Y), %xmm6
	movhps	 -6 * SIZE(Y), %xmm6
	mulps	%xmm4, %xmm3
	movaps	 -8 * SIZE(X), %xmm4
	addps	%xmm3, %xmm1

	PSHUFD2($0xb1, %xmm7, %xmm3)
	mulps	%xmm5, %xmm7
	addps	%xmm7, %xmm0
	movsd	 -4 * SIZE(Y), %xmm7
	movhps	 -2 * SIZE(Y), %xmm7
	mulps	%xmm5, %xmm3
	movaps	 -4 * SIZE(X), %xmm5
	addps	%xmm3, %xmm1

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	PSHUFD2($0xb1, %xmm6, %xmm3)
	mulps	%xmm4, %xmm6
	addps	%xmm6, %xmm0
	movsd	  0 * SIZE(Y), %xmm6
	movhps	  2 * SIZE(Y), %xmm6
	mulps	%xmm4, %xmm3
	movaps	  0 * SIZE(X), %xmm4
	addps	%xmm3, %xmm1

	PSHUFD2($0xb1, %xmm7, %xmm3)
	mulps	%xmm5, %xmm7
	addps	%xmm7, %xmm0
	movsd	  4 * SIZE(Y), %xmm7
	movhps	  6 * SIZE(Y), %xmm7
	mulps	%xmm5, %xmm3
	movaps	  4 * SIZE(X), %xmm5
	addps	%xmm3, %xmm1

	subl	$-32 * SIZE, X
	subl	$-32 * SIZE, Y

	decl	%eax
	jg	.L31
	ALIGN_3

.L32:
	PSHUFD2($0xb1, %xmm6, %xmm3)
	mulps	%xmm4, %xmm6
	addps	%xmm6, %xmm0
	movsd	-24 * SIZE(Y), %xmm6
	movhps	-22 * SIZE(Y), %xmm6
	mulps	%xmm4, %xmm3
	movaps	-24 * SIZE(X), %xmm4
	addps	%xmm3, %xmm1

	PSHUFD2($0xb1, %xmm7, %xmm3)
	mulps	%xmm5, %xmm7
	addps	%xmm7, %xmm0
	movsd	-20 * SIZE(Y), %xmm7
	movhps	-18 * SIZE(Y), %xmm7
	mulps	%xmm5, %xmm3
	movaps	-20 * SIZE(X), %xmm5
	addps	%xmm3, %xmm1

	PSHUFD2($0xb1, %xmm6, %xmm3)
	mulps	%xmm4, %xmm6
	addps	%xmm6, %xmm0
	movsd	-16 * SIZE(Y), %xmm6
	movhps	-14 * SIZE(Y), %xmm6
	mulps	%xmm4, %xmm3
	movaps	-16 * SIZE(X), %xmm4
	addps	%xmm3, %xmm1

	PSHUFD2($0xb1, %xmm7, %xmm3)
	mulps	%xmm5, %xmm7
	addps	%xmm7, %xmm0
	movsd	-12 * SIZE(Y), %xmm7
	movhps	-10 * SIZE(Y), %xmm7
	mulps	%xmm5, %xmm3
	movaps	-12 * SIZE(X), %xmm5
	addps	%xmm3, %xmm1

	PSHUFD2($0xb1, %xmm6, %xmm3)
	mulps	%xmm4, %xmm6
	addps	%xmm6, %xmm0
	movsd	 -8 * SIZE(Y), %xmm6
	movhps	 -6 * SIZE(Y), %xmm6
	mulps	%xmm4, %xmm3
	movaps	 -8 * SIZE(X), %xmm4
	addps	%xmm3, %xmm1

	PSHUFD2($0xb1, %xmm7, %xmm3)
	mulps	%xmm5, %xmm7
	addps	%xmm7, %xmm0
	movsd	 -4 * SIZE(Y), %xmm7
	movhps	 -2 * SIZE(Y), %xmm7
	mulps	%xmm5, %xmm3
	movaps	 -4 * SIZE(X), %xmm5
	addps	%xmm3, %xmm1

	PSHUFD2($0xb1, %xmm6, %xmm3)
	mulps	%xmm4, %xmm6
	addps	%xmm6, %xmm0
	mulps	%xmm4, %xmm3
	addps	%xmm3, %xmm1

	PSHUFD2($0xb1, %xmm7, %xmm3)
	mulps	%xmm5, %xmm7
	addps	%xmm7, %xmm0
	mulps	%xmm5, %xmm3
	addps	%xmm3, %xmm1

	subl	$-32 * SIZE, X
	subl	$-32 * SIZE, Y
	ALIGN_3

.L35:
	testl	$8, N
	jle	.L36

	movaps	-32 * SIZE(X), %xmm4
	movsd	-32 * SIZE(Y), %xmm6
	movhps	-30 * SIZE(Y), %xmm6

	movaps	-28 * SIZE(X), %xmm5
	movsd	-28 * SIZE(Y), %xmm7
	movhps	-26 * SIZE(Y), %xmm7

	PSHUFD2($0xb1, %xmm6, %xmm3)
	mulps	%xmm4, %xmm6
	addps	%xmm6, %xmm0
	movsd	-24 * SIZE(Y), %xmm6
	movhps	-22 * SIZE(Y), %xmm6
	mulps	%xmm4, %xmm3
	movaps	-24 * SIZE(X), %xmm4
	addps	%xmm3, %xmm1

	PSHUFD2($0xb1, %xmm7, %xmm3)
	mulps	%xmm5, %xmm7
	addps	%xmm7, %xmm0
	movsd	-20 * SIZE(Y), %xmm7
	movhps	-18 * SIZE(Y), %xmm7
	mulps	%xmm5, %xmm3
	movaps	-20 * SIZE(X), %xmm5
	addps	%xmm3, %xmm1

	PSHUFD2($0xb1, %xmm6, %xmm3)
	mulps	%xmm4, %xmm6
	addps	%xmm6, %xmm0
	mulps	%xmm4, %xmm3
	addps	%xmm3, %xmm1

	PSHUFD2($0xb1, %xmm7, %xmm3)
	mulps	%xmm5, %xmm7
	addps	%xmm7, %xmm0
	mulps	%xmm5, %xmm3
	addps	%xmm3, %xmm1

	addl	$16 * SIZE, X
	addl	$16 * SIZE, Y
	ALIGN_3

.L36:
	testl	$4, N
	jle	.L37

	movaps	-32 * SIZE(X), %xmm4
	movsd	-32 * SIZE(Y), %xmm6
	movhps	-30 * SIZE(Y), %xmm6

	PSHUFD2($0xb1, %xmm6, %xmm3)
	mulps	%xmm4, %xmm6
	addps	%xmm6, %xmm0
	mulps	%xmm4, %xmm3
	addps	%xmm3, %xmm1

	movaps	-28 * SIZE(X), %xmm5
	movsd	-28 * SIZE(Y), %xmm7
	movhps	-26 * SIZE(Y), %xmm7

	PSHUFD2($0xb1, %xmm7, %xmm3)
	mulps	%xmm5, %xmm7
	addps	%xmm7, %xmm0
	mulps	%xmm5, %xmm3
	addps	%xmm3, %xmm1

	addl	$8 * SIZE, X
	addl	$8 * SIZE, Y
	ALIGN_3

.L37:
	testl	$2, N
	jle	.L38

	movaps	-32 * SIZE(X), %xmm4
	movsd	-32 * SIZE(Y), %xmm6
	movhps	-30 * SIZE(Y), %xmm6

	PSHUFD2($0xb1, %xmm6, %xmm3)
	mulps	%xmm4, %xmm6
	addps	%xmm6, %xmm0
	mulps	%xmm4, %xmm3
	addps	%xmm3, %xmm1

	addl	$4 * SIZE, X
	addl	$4 * SIZE, Y
	ALIGN_3

.L38:
	testl	$1, N
	jle	.L98

#ifdef movsd
	xorps	%xmm4, %xmm4
#endif
	movsd	-32 * SIZE(X), %xmm4
#ifdef movsd
	xorps	%xmm6, %xmm6
#endif
	movsd	-32 * SIZE(Y), %xmm6

	PSHUFD2($0xb1, %xmm6, %xmm3)
	mulps	%xmm4, %xmm6
	addps	%xmm6, %xmm0
	mulps	%xmm4, %xmm3
	addps	%xmm3, %xmm1
	jmp	.L98
	ALIGN_3

#ifdef ALIGNED_ACCESS
.L40:
	movaps	-35 * SIZE(Y), %xmm6
	addl	$1 * SIZE, Y

	shufps	$0xb1, %xmm1, %xmm1

	movl	N,  %eax
	sarl	$4, %eax
	jle	.L45

	movaps	-32 * SIZE(X), %xmm4
	movaps	-28 * SIZE(X), %xmm5
	movaps	-32 * SIZE(Y), %xmm7

	decl	%eax
	jle	.L42
	ALIGN_3

.L41:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif


#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	movss	%xmm7, %xmm6
	PSHUFD2($0xb1, %xmm4, %xmm3)
	shufps	$0x93, %xmm7, %xmm6
	mulps	%xmm6, %xmm4
	addps	%xmm4, %xmm0
	movaps	-24 * SIZE(X), %xmm4
	mulps	%xmm6, %xmm3
	movaps	-28 * SIZE(Y), %xmm6
	addps	%xmm3, %xmm1

	movss	%xmm6, %xmm7
	PSHUFD2($0xb1, %xmm5, %xmm3)
	shufps	$0x93, %xmm6, %xmm7
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm0
	movaps	-20 * SIZE(X), %xmm5
	mulps	%xmm7, %xmm3
	movaps	-24 * SIZE(Y), %xmm7
	addps	%xmm3, %xmm1

	movss	%xmm7, %xmm6
	PSHUFD2($0xb1, %xmm4, %xmm3)
	shufps	$0x93, %xmm7, %xmm6
	mulps	%xmm6, %xmm4
	addps	%xmm4, %xmm0
	movaps	-16 * SIZE(X), %xmm4
	mulps	%xmm6, %xmm3
	movaps	-20 * SIZE(Y), %xmm6
	addps	%xmm3, %xmm1

	movss	%xmm6, %xmm7
	PSHUFD2($0xb1, %xmm5, %xmm3)
	shufps	$0x93, %xmm6, %xmm7
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm0
	movaps	-12 * SIZE(X), %xmm5
	mulps	%xmm7, %xmm3
	movaps	-16 * SIZE(Y), %xmm7
	addps	%xmm3, %xmm1

	movss	%xmm7, %xmm6
	PSHUFD2($0xb1, %xmm4, %xmm3)
	shufps	$0x93, %xmm7, %xmm6
	mulps	%xmm6, %xmm4
	addps	%xmm4, %xmm0
	movaps	 -8 * SIZE(X), %xmm4
	mulps	%xmm6, %xmm3
	movaps	-12 * SIZE(Y), %xmm6
	addps	%xmm3, %xmm1

	movss	%xmm6, %xmm7
	PSHUFD2($0xb1, %xmm5, %xmm3)
	shufps	$0x93, %xmm6, %xmm7
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm0
	movaps	 -4 * SIZE(X), %xmm5
	mulps	%xmm7, %xmm3
	movaps	 -8 * SIZE(Y), %xmm7
	addps	%xmm3, %xmm1

	movss	%xmm7, %xmm6
	PSHUFD2($0xb1, %xmm4, %xmm3)
	shufps	$0x93, %xmm7, %xmm6
	mulps	%xmm6, %xmm4
	addps	%xmm4, %xmm0
	movaps	  0 * SIZE(X), %xmm4
	mulps	%xmm6, %xmm3
	movaps	 -4 * SIZE(Y), %xmm6
	addps	%xmm3, %xmm1

	movss	%xmm6, %xmm7
	PSHUFD2($0xb1, %xmm5, %xmm3)
	shufps	$0x93, %xmm6, %xmm7
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm0
	movaps	  4 * SIZE(X), %xmm5
	mulps	%xmm7, %xmm3
	movaps	  0 * SIZE(Y), %xmm7
	addps	%xmm3, %xmm1

	subl	$-32 * SIZE, X
	subl	$-32 * SIZE, Y

	decl	%eax
	jg	.L41
	ALIGN_3

.L42:
	movss	%xmm7, %xmm6
	PSHUFD2($0xb1, %xmm4, %xmm3)
	shufps	$0x93, %xmm7, %xmm6
	mulps	%xmm6, %xmm4
	addps	%xmm4, %xmm0
	movaps	-24 * SIZE(X), %xmm4
	mulps	%xmm6, %xmm3
	movaps	-28 * SIZE(Y), %xmm6
	addps	%xmm3, %xmm1

	movss	%xmm6, %xmm7
	PSHUFD2($0xb1, %xmm5, %xmm3)
	shufps	$0x93, %xmm6, %xmm7
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm0
	movaps	-20 * SIZE(X), %xmm5
	mulps	%xmm7, %xmm3
	movaps	-24 * SIZE(Y), %xmm7
	addps	%xmm3, %xmm1

	movss	%xmm7, %xmm6
	PSHUFD2($0xb1, %xmm4, %xmm3)
	shufps	$0x93, %xmm7, %xmm6
	mulps	%xmm6, %xmm4
	addps	%xmm4, %xmm0
	movaps	-16 * SIZE(X), %xmm4
	mulps	%xmm6, %xmm3
	movaps	-20 * SIZE(Y), %xmm6
	addps	%xmm3, %xmm1

	movss	%xmm6, %xmm7
	PSHUFD2($0xb1, %xmm5, %xmm3)
	shufps	$0x93, %xmm6, %xmm7
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm0
	movaps	-12 * SIZE(X), %xmm5
	mulps	%xmm7, %xmm3
	movaps	-16 * SIZE(Y), %xmm7
	addps	%xmm3, %xmm1

	movss	%xmm7, %xmm6
	PSHUFD2($0xb1, %xmm4, %xmm3)
	shufps	$0x93, %xmm7, %xmm6
	mulps	%xmm6, %xmm4
	addps	%xmm4, %xmm0
	movaps	 -8 * SIZE(X), %xmm4
	mulps	%xmm6, %xmm3
	movaps	-12 * SIZE(Y), %xmm6
	addps	%xmm3, %xmm1

	movss	%xmm6, %xmm7
	PSHUFD2($0xb1, %xmm5, %xmm3)
	shufps	$0x93, %xmm6, %xmm7
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm0
	movaps	 -4 * SIZE(X), %xmm5
	mulps	%xmm7, %xmm3
	movaps	 -8 * SIZE(Y), %xmm7
	addps	%xmm3, %xmm1

	movss	%xmm7, %xmm6
	PSHUFD2($0xb1, %xmm4, %xmm3)
	shufps	$0x93, %xmm7, %xmm6
	mulps	%xmm6, %xmm4
	addps	%xmm4, %xmm0
	mulps	%xmm6, %xmm3
	movaps	 -4 * SIZE(Y), %xmm6
	addps	%xmm3, %xmm1

	movss	%xmm6, %xmm7
	PSHUFD2($0xb1, %xmm5, %xmm3)
	shufps	$0x93, %xmm6, %xmm7
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm0
	mulps	%xmm7, %xmm3
	addps	%xmm3, %xmm1

	subl	$-32 * SIZE, X
	subl	$-32 * SIZE, Y
	ALIGN_3

.L45:
	testl	$8, N
	jle	.L46

	movaps	-32 * SIZE(X), %xmm4
	movaps	-28 * SIZE(X), %xmm5
	movaps	-32 * SIZE(Y), %xmm7

	movss	%xmm7, %xmm6
	PSHUFD2($0xb1, %xmm4, %xmm3)
	shufps	$0x93, %xmm7, %xmm6
	mulps	%xmm6, %xmm4
	addps	%xmm4, %xmm0
	movaps	-24 * SIZE(X), %xmm4
	mulps	%xmm6, %xmm3
	movaps	-28 * SIZE(Y), %xmm6
	addps	%xmm3, %xmm1

	movss	%xmm6, %xmm7
	PSHUFD2($0xb1, %xmm5, %xmm3)
	shufps	$0x93, %xmm6, %xmm7
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm0
	movaps	-20 * SIZE(X), %xmm5
	mulps	%xmm7, %xmm3
	movaps	-24 * SIZE(Y), %xmm7
	addps	%xmm3, %xmm1

	movss	%xmm7, %xmm6
	PSHUFD2($0xb1, %xmm4, %xmm3)
	shufps	$0x93, %xmm7, %xmm6
	mulps	%xmm6, %xmm4
	addps	%xmm4, %xmm0
	mulps	%xmm6, %xmm3
	movaps	-20 * SIZE(Y), %xmm6
	addps	%xmm3, %xmm1

	movss	%xmm6, %xmm7
	PSHUFD2($0xb1, %xmm5, %xmm3)
	shufps	$0x93, %xmm6, %xmm7
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm0
	mulps	%xmm7, %xmm3
	addps	%xmm3, %xmm1

	addl	$16 * SIZE, X
	addl	$16 * SIZE, Y
	ALIGN_3

.L46:
	testl	$4, N
	jle	.L47

	movaps	-32 * SIZE(X), %xmm4
	movaps	-28 * SIZE(X), %xmm5
	movaps	-32 * SIZE(Y), %xmm7

	movss	%xmm7, %xmm6
	PSHUFD2($0xb1, %xmm4, %xmm3)
	shufps	$0x93, %xmm7, %xmm6
	mulps	%xmm6, %xmm4
	addps	%xmm4, %xmm0
	mulps	%xmm6, %xmm3
	movaps	-28 * SIZE(Y), %xmm6
	addps	%xmm3, %xmm1

	movss	%xmm6, %xmm7
	PSHUFD2($0xb1, %xmm5, %xmm3)
	shufps	$0x93, %xmm6, %xmm7
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm0
	mulps	%xmm7, %xmm3
	addps	%xmm3, %xmm1

	addl	$8 * SIZE, X
	addl	$8 * SIZE, Y
	ALIGN_3

.L47:
	testl	$2, N
	jle	.L48

	movaps	-32 * SIZE(X), %xmm4
	movaps	-32 * SIZE(Y), %xmm7

	movss	%xmm7, %xmm6
	PSHUFD2($0xb1, %xmm4, %xmm3)
	shufps	$0x93, %xmm7, %xmm6
	mulps	%xmm6, %xmm4
	addps	%xmm4, %xmm0
	mulps	%xmm6, %xmm3
	addps	%xmm3, %xmm1

	movaps	%xmm7, %xmm6
	addl	$4 * SIZE, X
	addl	$4 * SIZE, Y
	ALIGN_3

.L48:
	testl	$1, N
	jle	.L49

#ifdef movsd
	xorps	%xmm4, %xmm4
#endif
	movsd	-32 * SIZE(X), %xmm4
	movss	-32 * SIZE(Y), %xmm7

	movss	%xmm7, %xmm6
	PSHUFD2($0xb1, %xmm4, %xmm3)
	shufps	$0x93, %xmm6, %xmm6
	mulps	%xmm6, %xmm4
	addps	%xmm4, %xmm0
	mulps	%xmm6, %xmm3
	addps	%xmm3, %xmm1
	ALIGN_3

.L49:
	shufps	$0xb1, %xmm1, %xmm1
	jmp	.L98
	ALIGN_3
#endif

.L50:
	testl	$SIZE, Y
	jne	.L70

#ifdef ALIGNED_ACCESS

	testl	$2 * SIZE, Y
	je	.L50x

#ifdef movsd
	xorps	%xmm0, %xmm0
#endif
	movsd	-32 * SIZE(X), %xmm0
#ifdef movsd
	xorps	%xmm4, %xmm4
#endif
	movsd	-32 * SIZE(Y), %xmm4

	PSHUFD2($0xb1, %xmm0, %xmm1)
	mulps	%xmm4, %xmm0
	mulps	%xmm4, %xmm1
	addl	$2 * SIZE, X
	addl	$2 * SIZE, Y

	decl	N
	ALIGN_3

.L50x:
	testl	$2 * SIZE, X
	jne	.L60

	movaps	-33 * SIZE(X), %xmm6
	addl	$3 * SIZE, X

	shufps	$0xb1, %xmm1, %xmm1

	movl	N,  %eax
	sarl	$4, %eax
	jle	.L55

	movaps	-32 * SIZE(Y), %xmm4
	movaps	-28 * SIZE(Y), %xmm5
	movaps	-32 * SIZE(X), %xmm7

	decl	%eax
	jle	.L52
	ALIGN_3

.L51:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movss	%xmm7, %xmm6
	PSHUFD2($0xb1, %xmm4, %xmm3)
	shufps	$0x39, %xmm6, %xmm6
	mulps	%xmm6, %xmm4
	addps	%xmm4, %xmm0
	movaps	-24 * SIZE(Y), %xmm4
	mulps	%xmm6, %xmm3
	movaps	-28 * SIZE(X), %xmm6
	addps	%xmm3, %xmm1

	movss	%xmm6, %xmm7
	PSHUFD2($0xb1, %xmm5, %xmm3)
	shufps	$0x39, %xmm7, %xmm7
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm0
	movaps	-20 * SIZE(Y), %xmm5
	mulps	%xmm7, %xmm3
	movaps	-24 * SIZE(X), %xmm7
	addps	%xmm3, %xmm1

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	movss	%xmm7, %xmm6
	PSHUFD2($0xb1, %xmm4, %xmm3)
	shufps	$0x39, %xmm6, %xmm6
	mulps	%xmm6, %xmm4
	addps	%xmm4, %xmm0
	movaps	-16 * SIZE(Y), %xmm4
	mulps	%xmm6, %xmm3
	movaps	-20 * SIZE(X), %xmm6
	addps	%xmm3, %xmm1

	movss	%xmm6, %xmm7
	PSHUFD2($0xb1, %xmm5, %xmm3)
	shufps	$0x39, %xmm7, %xmm7
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm0
	movaps	-12 * SIZE(Y), %xmm5
	mulps	%xmm7, %xmm3
	movaps	-16 * SIZE(X), %xmm7
	addps	%xmm3, %xmm1

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	movss	%xmm7, %xmm6
	PSHUFD2($0xb1, %xmm4, %xmm3)
	shufps	$0x39, %xmm6, %xmm6
	mulps	%xmm6, %xmm4
	addps	%xmm4, %xmm0
	movaps	 -8 * SIZE(Y), %xmm4
	mulps	%xmm6, %xmm3
	movaps	-12 * SIZE(X), %xmm6
	addps	%xmm3, %xmm1

	movss	%xmm6, %xmm7
	PSHUFD2($0xb1, %xmm5, %xmm3)
	shufps	$0x39, %xmm7, %xmm7
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm0
	movaps	 -4 * SIZE(Y), %xmm5
	mulps	%xmm7, %xmm3
	movaps	 -8 * SIZE(X), %xmm7
	addps	%xmm3, %xmm1

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	movss	%xmm7, %xmm6
	PSHUFD2($0xb1, %xmm4, %xmm3)
	shufps	$0x39, %xmm6, %xmm6
	mulps	%xmm6, %xmm4
	addps	%xmm4, %xmm0
	movaps	  0 * SIZE(Y), %xmm4
	mulps	%xmm6, %xmm3
	movaps	 -4 * SIZE(X), %xmm6
	addps	%xmm3, %xmm1

	movss	%xmm6, %xmm7
	PSHUFD2($0xb1, %xmm5, %xmm3)
	shufps	$0x39, %xmm7, %xmm7
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm0
	movaps	  4 * SIZE(Y), %xmm5
	mulps	%xmm7, %xmm3
	movaps	  0 * SIZE(X), %xmm7
	addps	%xmm3, %xmm1

	subl	$-32 * SIZE, X
	subl	$-32 * SIZE, Y

	decl	%eax
	jg	.L51
	ALIGN_3

.L52:
	movss	%xmm7, %xmm6
	PSHUFD2($0xb1, %xmm4, %xmm3)
	shufps	$0x39, %xmm6, %xmm6
	mulps	%xmm6, %xmm4
	addps	%xmm4, %xmm0
	movaps	-24 * SIZE(Y), %xmm4
	mulps	%xmm6, %xmm3
	movaps	-28 * SIZE(X), %xmm6
	addps	%xmm3, %xmm1

	movss	%xmm6, %xmm7
	PSHUFD2($0xb1, %xmm5, %xmm3)
	shufps	$0x39, %xmm7, %xmm7
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm0
	movaps	-20 * SIZE(Y), %xmm5
	mulps	%xmm7, %xmm3
	movaps	-24 * SIZE(X), %xmm7
	addps	%xmm3, %xmm1

	movss	%xmm7, %xmm6
	PSHUFD2($0xb1, %xmm4, %xmm3)
	shufps	$0x39, %xmm6, %xmm6
	mulps	%xmm6, %xmm4
	addps	%xmm4, %xmm0
	movaps	-16 * SIZE(Y), %xmm4
	mulps	%xmm6, %xmm3
	movaps	-20 * SIZE(X), %xmm6
	addps	%xmm3, %xmm1

	movss	%xmm6, %xmm7
	PSHUFD2($0xb1, %xmm5, %xmm3)
	shufps	$0x39, %xmm7, %xmm7
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm0
	movaps	-12 * SIZE(Y), %xmm5
	mulps	%xmm7, %xmm3
	movaps	-16 * SIZE(X), %xmm7
	addps	%xmm3, %xmm1

	movss	%xmm7, %xmm6
	PSHUFD2($0xb1, %xmm4, %xmm3)
	shufps	$0x39, %xmm6, %xmm6
	mulps	%xmm6, %xmm4
	addps	%xmm4, %xmm0
	movaps	 -8 * SIZE(Y), %xmm4
	mulps	%xmm6, %xmm3
	movaps	-12 * SIZE(X), %xmm6
	addps	%xmm3, %xmm1

	movss	%xmm6, %xmm7
	PSHUFD2($0xb1, %xmm5, %xmm3)
	shufps	$0x39, %xmm7, %xmm7
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm0
	movaps	 -4 * SIZE(Y), %xmm5
	mulps	%xmm7, %xmm3
	movaps	 -8 * SIZE(X), %xmm7
	addps	%xmm3, %xmm1

	movss	%xmm7, %xmm6
	PSHUFD2($0xb1, %xmm4, %xmm3)
	shufps	$0x39, %xmm6, %xmm6
	mulps	%xmm6, %xmm4
	addps	%xmm4, %xmm0
	mulps	%xmm6, %xmm3
	movaps	 -4 * SIZE(X), %xmm6
	addps	%xmm3, %xmm1

	movss	%xmm6, %xmm7
	PSHUFD2($0xb1, %xmm5, %xmm3)
	shufps	$0x39, %xmm7, %xmm7
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm0
	mulps	%xmm7, %xmm3
	addps	%xmm3, %xmm1

	subl	$-32 * SIZE, X
	subl	$-32 * SIZE, Y
	ALIGN_3

.L55:
	testl	$8, N
	jle	.L56

	movaps	-32 * SIZE(Y), %xmm4
	movaps	-28 * SIZE(Y), %xmm5
	movaps	-32 * SIZE(X), %xmm7

	movss	%xmm7, %xmm6
	PSHUFD2($0xb1, %xmm4, %xmm3)
	shufps	$0x39, %xmm6, %xmm6
	mulps	%xmm6, %xmm4
	addps	%xmm4, %xmm0
	movaps	-24 * SIZE(Y), %xmm4
	mulps	%xmm6, %xmm3
	movaps	-28 * SIZE(X), %xmm6
	addps	%xmm3, %xmm1

	movss	%xmm6, %xmm7
	PSHUFD2($0xb1, %xmm5, %xmm3)
	shufps	$0x39, %xmm7, %xmm7
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm0
	movaps	-20 * SIZE(Y), %xmm5
	mulps	%xmm7, %xmm3
	movaps	-24 * SIZE(X), %xmm7
	addps	%xmm3, %xmm1

	movss	%xmm7, %xmm6
	PSHUFD2($0xb1, %xmm4, %xmm3)
	shufps	$0x39, %xmm6, %xmm6
	mulps	%xmm6, %xmm4
	addps	%xmm4, %xmm0
	mulps	%xmm6, %xmm3
	movaps	-20 * SIZE(X), %xmm6
	addps	%xmm3, %xmm1

	movss	%xmm6, %xmm7
	PSHUFD2($0xb1, %xmm5, %xmm3)
	shufps	$0x39, %xmm7, %xmm7
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm0
	mulps	%xmm7, %xmm3
	addps	%xmm3, %xmm1

	addl	$16 * SIZE, X
	addl	$16 * SIZE, Y
	ALIGN_3

.L56:
	testl	$4, N
	jle	.L57

	movaps	-32 * SIZE(Y), %xmm4
	movaps	-28 * SIZE(Y), %xmm5
	movaps	-32 * SIZE(X), %xmm7

	movss	%xmm7, %xmm6
	PSHUFD2($0xb1, %xmm4, %xmm3)
	shufps	$0x39, %xmm6, %xmm6
	mulps	%xmm6, %xmm4
	addps	%xmm4, %xmm0
	mulps	%xmm6, %xmm3
	movaps	-28 * SIZE(X), %xmm6
	addps	%xmm3, %xmm1

	movss	%xmm6, %xmm7
	PSHUFD2($0xb1, %xmm5, %xmm3)
	shufps	$0x39, %xmm7, %xmm7
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm0
	mulps	%xmm7, %xmm3
	addps	%xmm3, %xmm1

	addl	$8 * SIZE, X
	addl	$8 * SIZE, Y
	ALIGN_3

.L57:
	testl	$2, N
	jle	.L58

	movaps	-32 * SIZE(Y), %xmm4
	movaps	-32 * SIZE(X), %xmm7

	movss	%xmm7, %xmm6
	PSHUFD2($0xb1, %xmm4, %xmm3)
	shufps	$0x39, %xmm6, %xmm6
	mulps	%xmm6, %xmm4
	addps	%xmm4, %xmm0
	mulps	%xmm6, %xmm3
	addps	%xmm3, %xmm1

	movaps	%xmm7, %xmm6
	addl	$4 * SIZE, X
	addl	$4 * SIZE, Y
	ALIGN_3

.L58:
	testl	$1, N
	jle	.L98

#ifdef movsd
	xorps	%xmm4, %xmm4
#endif
	movsd	-32 * SIZE(Y), %xmm4

	PSHUFD2($0xb1, %xmm4, %xmm3)
	shufps	$0x39, %xmm6, %xmm6
	mulps	%xmm6, %xmm4
	addps	%xmm4, %xmm0
	mulps	%xmm6, %xmm3
	addps	%xmm3, %xmm1
	jmp	.L98
	ALIGN_3

.L60:
	movaps	-35 * SIZE(X), %xmm6
	addl	$1 * SIZE, X

	shufps	$0xb1, %xmm1, %xmm1

	movl	N,  %eax
	sarl	$4, %eax
	jle	.L65

	movaps	-32 * SIZE(Y), %xmm4
	movaps	-28 * SIZE(Y), %xmm5
	movaps	-32 * SIZE(X), %xmm7

	decl	%eax
	jle	.L62
	ALIGN_3

.L61:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movss	%xmm7, %xmm6
	PSHUFD2($0xb1, %xmm4, %xmm3)
	shufps	$0x93, %xmm7, %xmm6
	mulps	%xmm6, %xmm4
	addps	%xmm4, %xmm0
	movaps	-24 * SIZE(Y), %xmm4
	mulps	%xmm6, %xmm3
	movaps	-28 * SIZE(X), %xmm6
	addps	%xmm3, %xmm1

	movss	%xmm6, %xmm7
	PSHUFD2($0xb1, %xmm5, %xmm3)
	shufps	$0x93, %xmm6, %xmm7
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm0
	movaps	-20 * SIZE(Y), %xmm5
	mulps	%xmm7, %xmm3
	movaps	-24 * SIZE(X), %xmm7
	addps	%xmm3, %xmm1

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	movss	%xmm7, %xmm6
	PSHUFD2($0xb1, %xmm4, %xmm3)
	shufps	$0x93, %xmm7, %xmm6
	mulps	%xmm6, %xmm4
	addps	%xmm4, %xmm0
	movaps	-16 * SIZE(Y), %xmm4
	mulps	%xmm6, %xmm3
	movaps	-20 * SIZE(X), %xmm6
	addps	%xmm3, %xmm1

	movss	%xmm6, %xmm7
	PSHUFD2($0xb1, %xmm5, %xmm3)
	shufps	$0x93, %xmm6, %xmm7
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm0
	movaps	-12 * SIZE(Y), %xmm5
	mulps	%xmm7, %xmm3
	movaps	-16 * SIZE(X), %xmm7
	addps	%xmm3, %xmm1

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	movss	%xmm7, %xmm6
	PSHUFD2($0xb1, %xmm4, %xmm3)
	shufps	$0x93, %xmm7, %xmm6
	mulps	%xmm6, %xmm4
	addps	%xmm4, %xmm0
	movaps	 -8 * SIZE(Y), %xmm4
	mulps	%xmm6, %xmm3
	movaps	-12 * SIZE(X), %xmm6
	addps	%xmm3, %xmm1

	movss	%xmm6, %xmm7
	PSHUFD2($0xb1, %xmm5, %xmm3)
	shufps	$0x93, %xmm6, %xmm7
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm0
	movaps	 -4 * SIZE(Y), %xmm5
	mulps	%xmm7, %xmm3
	movaps	 -8 * SIZE(X), %xmm7
	addps	%xmm3, %xmm1

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	movss	%xmm7, %xmm6
	PSHUFD2($0xb1, %xmm4, %xmm3)
	shufps	$0x93, %xmm7, %xmm6
	mulps	%xmm6, %xmm4
	addps	%xmm4, %xmm0
	movaps	  0 * SIZE(Y), %xmm4
	mulps	%xmm6, %xmm3
	movaps	 -4 * SIZE(X), %xmm6
	addps	%xmm3, %xmm1

	movss	%xmm6, %xmm7
	PSHUFD2($0xb1, %xmm5, %xmm3)
	shufps	$0x93, %xmm6, %xmm7
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm0
	movaps	  4 * SIZE(Y), %xmm5
	mulps	%xmm7, %xmm3
	movaps	  0 * SIZE(X), %xmm7
	addps	%xmm3, %xmm1

	subl	$-32 * SIZE, X
	subl	$-32 * SIZE, Y

	decl	%eax
	jg	.L61
	ALIGN_3

.L62:
	movss	%xmm7, %xmm6
	PSHUFD2($0xb1, %xmm4, %xmm3)
	shufps	$0x93, %xmm7, %xmm6
	mulps	%xmm6, %xmm4
	addps	%xmm4, %xmm0
	movaps	-24 * SIZE(Y), %xmm4
	mulps	%xmm6, %xmm3
	movaps	-28 * SIZE(X), %xmm6
	addps	%xmm3, %xmm1

	movss	%xmm6, %xmm7
	PSHUFD2($0xb1, %xmm5, %xmm3)
	shufps	$0x93, %xmm6, %xmm7
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm0
	movaps	-20 * SIZE(Y), %xmm5
	mulps	%xmm7, %xmm3
	movaps	-24 * SIZE(X), %xmm7
	addps	%xmm3, %xmm1

	movss	%xmm7, %xmm6
	PSHUFD2($0xb1, %xmm4, %xmm3)
	shufps	$0x93, %xmm7, %xmm6
	mulps	%xmm6, %xmm4
	addps	%xmm4, %xmm0
	movaps	-16 * SIZE(Y), %xmm4
	mulps	%xmm6, %xmm3
	movaps	-20 * SIZE(X), %xmm6
	addps	%xmm3, %xmm1

	movss	%xmm6, %xmm7
	PSHUFD2($0xb1, %xmm5, %xmm3)
	shufps	$0x93, %xmm6, %xmm7
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm0
	movaps	-12 * SIZE(Y), %xmm5
	mulps	%xmm7, %xmm3
	movaps	-16 * SIZE(X), %xmm7
	addps	%xmm3, %xmm1

	movss	%xmm7, %xmm6
	PSHUFD2($0xb1, %xmm4, %xmm3)
	shufps	$0x93, %xmm7, %xmm6
	mulps	%xmm6, %xmm4
	addps	%xmm4, %xmm0
	movaps	 -8 * SIZE(Y), %xmm4
	mulps	%xmm6, %xmm3
	movaps	-12 * SIZE(X), %xmm6
	addps	%xmm3, %xmm1

	movss	%xmm6, %xmm7
	PSHUFD2($0xb1, %xmm5, %xmm3)
	shufps	$0x93, %xmm6, %xmm7
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm0
	movaps	 -4 * SIZE(Y), %xmm5
	mulps	%xmm7, %xmm3
	movaps	 -8 * SIZE(X), %xmm7
	addps	%xmm3, %xmm1

	movss	%xmm7, %xmm6
	PSHUFD2($0xb1, %xmm4, %xmm3)
	shufps	$0x93, %xmm7, %xmm6
	mulps	%xmm6, %xmm4
	addps	%xmm4, %xmm0
	mulps	%xmm6, %xmm3
	movaps	 -4 * SIZE(X), %xmm6
	addps	%xmm3, %xmm1

	movss	%xmm6, %xmm7
	PSHUFD2($0xb1, %xmm5, %xmm3)
	shufps	$0x93, %xmm6, %xmm7
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm0
	mulps	%xmm7, %xmm3
	addps	%xmm3, %xmm1

	subl	$-32 * SIZE, X
	subl	$-32 * SIZE, Y
	ALIGN_3

.L65:
	testl	$8, N
	jle	.L66

	movaps	-32 * SIZE(Y), %xmm4
	movaps	-28 * SIZE(Y), %xmm5
	movaps	-32 * SIZE(X), %xmm7

	movss	%xmm7, %xmm6
	PSHUFD2($0xb1, %xmm4, %xmm3)
	shufps	$0x93, %xmm7, %xmm6
	mulps	%xmm6, %xmm4
	addps	%xmm4, %xmm0
	movaps	-24 * SIZE(Y), %xmm4
	mulps	%xmm6, %xmm3
	movaps	-28 * SIZE(X), %xmm6
	addps	%xmm3, %xmm1

	movss	%xmm6, %xmm7
	PSHUFD2($0xb1, %xmm5, %xmm3)
	shufps	$0x93, %xmm6, %xmm7
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm0
	movaps	-20 * SIZE(Y), %xmm5
	mulps	%xmm7, %xmm3
	movaps	-24 * SIZE(X), %xmm7
	addps	%xmm3, %xmm1

	movss	%xmm7, %xmm6
	PSHUFD2($0xb1, %xmm4, %xmm3)
	shufps	$0x93, %xmm7, %xmm6
	mulps	%xmm6, %xmm4
	addps	%xmm4, %xmm0
	mulps	%xmm6, %xmm3
	movaps	-20 * SIZE(X), %xmm6
	addps	%xmm3, %xmm1

	movss	%xmm6, %xmm7
	PSHUFD2($0xb1, %xmm5, %xmm3)
	shufps	$0x93, %xmm6, %xmm7
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm0
	mulps	%xmm7, %xmm3
	addps	%xmm3, %xmm1

	addl	$16 * SIZE, X
	addl	$16 * SIZE, Y
	ALIGN_3

.L66:
	testl	$4, N
	jle	.L67

	movaps	-32 * SIZE(Y), %xmm4
	movaps	-28 * SIZE(Y), %xmm5
	movaps	-32 * SIZE(X), %xmm7

	movss	%xmm7, %xmm6
	PSHUFD2($0xb1, %xmm4, %xmm3)
	shufps	$0x93, %xmm7, %xmm6
	mulps	%xmm6, %xmm4
	addps	%xmm4, %xmm0
	mulps	%xmm6, %xmm3
	movaps	-28 * SIZE(X), %xmm6
	addps	%xmm3, %xmm1

	movss	%xmm6, %xmm7
	PSHUFD2($0xb1, %xmm5, %xmm3)
	shufps	$0x93, %xmm6, %xmm7
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm0
	mulps	%xmm7, %xmm3
	addps	%xmm3, %xmm1

	addl	$8 * SIZE, X
	addl	$8 * SIZE, Y
	ALIGN_3

.L67:
	testl	$2, N
	jle	.L68

	movaps	-32 * SIZE(Y), %xmm4
	movaps	-32 * SIZE(X), %xmm7

	movss	%xmm7, %xmm6
	PSHUFD2($0xb1, %xmm4, %xmm3)
	shufps	$0x93, %xmm7, %xmm6
	mulps	%xmm6, %xmm4
	addps	%xmm4, %xmm0
	mulps	%xmm6, %xmm3
	addps	%xmm3, %xmm1

	movaps	%xmm7, %xmm6
	addl	$4 * SIZE, X
	addl	$4 * SIZE, Y
	ALIGN_3

.L68:
	testl	$1, N
	jle	.L98

#ifdef movsd
	xorps	%xmm4, %xmm4
#endif
	movsd	-32 * SIZE(Y), %xmm4
	movss	-32 * SIZE(X), %xmm7

	movss	%xmm7, %xmm6
	PSHUFD2($0xb1, %xmm4, %xmm3)
	shufps	$0x93, %xmm6, %xmm6
	mulps	%xmm6, %xmm4
	addps	%xmm4, %xmm0
	mulps	%xmm6, %xmm3
	addps	%xmm3, %xmm1
	jmp	.L98
	ALIGN_3

#else

	testl	$2 * SIZE, Y
	je	.L50x

#ifdef movsd
	xorps	%xmm0, %xmm0
#endif
	movsd	-32 * SIZE(Y), %xmm0
#ifdef movsd
	xorps	%xmm4, %xmm4
#endif
	movsd	-32 * SIZE(X), %xmm4

	PSHUFD2($0xb1, %xmm0, %xmm1)
	mulps	%xmm4, %xmm0
	mulps	%xmm4, %xmm1
	addl	$2 * SIZE, X
	addl	$2 * SIZE, Y

	decl	N
	ALIGN_3

.L50x:
	movl	N,  %eax
	sarl	$4, %eax
	jle	.L55

	movaps	-32 * SIZE(Y), %xmm4
	movlps	-32 * SIZE(X), %xmm6
	movhps	-30 * SIZE(X), %xmm6
	movaps	-28 * SIZE(Y), %xmm5
	movlps	-28 * SIZE(X), %xmm7
	movhps	-26 * SIZE(X), %xmm7

	decl	%eax
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

	PSHUFD2($0xb1, %xmm4, %xmm3)
	mulps	%xmm6, %xmm4
	addps	%xmm4, %xmm0
	movaps	-24 * SIZE(Y), %xmm4
	mulps	%xmm6, %xmm3
	movlps	-24 * SIZE(X), %xmm6
	movhps	-22 * SIZE(X), %xmm6
	addps	%xmm3, %xmm1

	PSHUFD2($0xb1, %xmm5, %xmm3)
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm0
	movaps	-20 * SIZE(Y), %xmm5
	mulps	%xmm7, %xmm3
	movlps	-20 * SIZE(X), %xmm7
	movhps	-18 * SIZE(X), %xmm7
	addps	%xmm3, %xmm1

	PSHUFD2($0xb1, %xmm4, %xmm3)
	mulps	%xmm6, %xmm4
	addps	%xmm4, %xmm0
	movaps	-16 * SIZE(Y), %xmm4
	mulps	%xmm6, %xmm3
	movlps	-16 * SIZE(X), %xmm6
	movhps	-14 * SIZE(X), %xmm6
	addps	%xmm3, %xmm1

	PSHUFD2($0xb1, %xmm5, %xmm3)
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm0
	movaps	-12 * SIZE(Y), %xmm5
	mulps	%xmm7, %xmm3
	movlps	-12 * SIZE(X), %xmm7
	movhps	-10 * SIZE(X), %xmm7
	addps	%xmm3, %xmm1

	PSHUFD2($0xb1, %xmm4, %xmm3)
	mulps	%xmm6, %xmm4
	addps	%xmm4, %xmm0
	movaps	 -8 * SIZE(Y), %xmm4
	mulps	%xmm6, %xmm3
	movlps	 -8 * SIZE(X), %xmm6
	movhps	 -6 * SIZE(X), %xmm6
	addps	%xmm3, %xmm1

	PSHUFD2($0xb1, %xmm5, %xmm3)
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm0
	movaps	-20 * SIZE(Y), %xmm5
	mulps	%xmm7, %xmm3
	movlps	-20 * SIZE(X), %xmm7
	movhps	-18 * SIZE(X), %xmm7
	addps	%xmm3, %xmm1

	PSHUFD2($0xb1, %xmm4, %xmm3)
	mulps	%xmm6, %xmm4
	addps	%xmm4, %xmm0
	movaps	  0 * SIZE(Y), %xmm4
	mulps	%xmm6, %xmm3
	movlps	  0 * SIZE(X), %xmm6
	movhps	  2 * SIZE(X), %xmm6
	addps	%xmm3, %xmm1

	PSHUFD2($0xb1, %xmm5, %xmm3)
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm0
	movaps	  4 * SIZE(Y), %xmm5
	mulps	%xmm7, %xmm3
	movlps	  4 * SIZE(X), %xmm7
	movhps	  6 * SIZE(X), %xmm7
	addps	%xmm3, %xmm1

	subl	$-32 * SIZE, X
	subl	$-32 * SIZE, Y

	decl	%eax
	jg	.L51
	ALIGN_3

.L52:
	PSHUFD2($0xb1, %xmm4, %xmm3)
	mulps	%xmm6, %xmm4
	addps	%xmm4, %xmm0
	movaps	-24 * SIZE(Y), %xmm4
	mulps	%xmm6, %xmm3
	movlps	-24 * SIZE(X), %xmm6
	movhps	-22 * SIZE(X), %xmm6
	addps	%xmm3, %xmm1

	PSHUFD2($0xb1, %xmm5, %xmm3)
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm0
	movaps	-20 * SIZE(Y), %xmm5
	mulps	%xmm7, %xmm3
	movlps	-20 * SIZE(X), %xmm7
	movhps	-18 * SIZE(X), %xmm7
	addps	%xmm3, %xmm1

	PSHUFD2($0xb1, %xmm4, %xmm3)
	mulps	%xmm6, %xmm4
	addps	%xmm4, %xmm0
	movaps	-16 * SIZE(Y), %xmm4
	mulps	%xmm6, %xmm3
	movlps	-16 * SIZE(X), %xmm6
	movhps	-14 * SIZE(X), %xmm6
	addps	%xmm3, %xmm1

	PSHUFD2($0xb1, %xmm5, %xmm3)
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm0
	movaps	-12 * SIZE(Y), %xmm5
	mulps	%xmm7, %xmm3
	movlps	-12 * SIZE(X), %xmm7
	movhps	-10 * SIZE(X), %xmm7
	addps	%xmm3, %xmm1

	PSHUFD2($0xb1, %xmm4, %xmm3)
	mulps	%xmm6, %xmm4
	addps	%xmm4, %xmm0
	movaps	 -8 * SIZE(Y), %xmm4
	mulps	%xmm6, %xmm3
	movlps	 -8 * SIZE(X), %xmm6
	movhps	 -6 * SIZE(X), %xmm6
	addps	%xmm3, %xmm1

	PSHUFD2($0xb1, %xmm5, %xmm3)
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm0
	movaps	-20 * SIZE(Y), %xmm5
	mulps	%xmm7, %xmm3
	movlps	-20 * SIZE(X), %xmm7
	movhps	-18 * SIZE(X), %xmm7
	addps	%xmm3, %xmm1

	PSHUFD2($0xb1, %xmm4, %xmm3)
	mulps	%xmm6, %xmm4
	addps	%xmm4, %xmm0
	mulps	%xmm6, %xmm3
	addps	%xmm3, %xmm1

	PSHUFD2($0xb1, %xmm5, %xmm3)
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm0
	mulps	%xmm7, %xmm3
	addps	%xmm3, %xmm1

	subl	$-32 * SIZE, X
	subl	$-32 * SIZE, Y
	ALIGN_3

.L55:
	testl	$8, N
	jle	.L56

	movaps	-32 * SIZE(Y), %xmm4
	movlps	-32 * SIZE(X), %xmm6
	movhps	-30 * SIZE(X), %xmm6

	movaps	-28 * SIZE(Y), %xmm5
	movlps	-28 * SIZE(X), %xmm7
	movhps	-26 * SIZE(X), %xmm7

	PSHUFD2($0xb1, %xmm4, %xmm3)
	mulps	%xmm6, %xmm4
	addps	%xmm4, %xmm0
	movaps	-24 * SIZE(Y), %xmm4
	mulps	%xmm6, %xmm3
	movlps	-24 * SIZE(X), %xmm6
	movhps	-22 * SIZE(X), %xmm6
	addps	%xmm3, %xmm1

	PSHUFD2($0xb1, %xmm5, %xmm3)
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm0
	movaps	-20 * SIZE(Y), %xmm5
	mulps	%xmm7, %xmm3
	movlps	-20 * SIZE(X), %xmm7
	movhps	-18 * SIZE(X), %xmm7
	addps	%xmm3, %xmm1

	PSHUFD2($0xb1, %xmm4, %xmm3)
	mulps	%xmm6, %xmm4
	addps	%xmm4, %xmm0
	mulps	%xmm6, %xmm3
	addps	%xmm3, %xmm1

	PSHUFD2($0xb1, %xmm5, %xmm3)
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm0
	mulps	%xmm7, %xmm3
	addps	%xmm3, %xmm1

	addl	$16 * SIZE, X
	addl	$16 * SIZE, Y
	ALIGN_3

.L56:
	testl	$4, N
	jle	.L57

	movaps	-32 * SIZE(Y), %xmm4
	movlps	-32 * SIZE(X), %xmm6
	movhps	-30 * SIZE(X), %xmm6

	PSHUFD2($0xb1, %xmm4, %xmm3)
	mulps	%xmm6, %xmm4
	addps	%xmm4, %xmm0
	mulps	%xmm6, %xmm3
	addps	%xmm3, %xmm1

	movaps	-28 * SIZE(Y), %xmm5
	movlps	-28 * SIZE(X), %xmm7
	movhps	-26 * SIZE(X), %xmm7

	PSHUFD2($0xb1, %xmm5, %xmm3)
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm0
	mulps	%xmm7, %xmm3
	addps	%xmm3, %xmm1

	addl	$8 * SIZE, X
	addl	$8 * SIZE, Y
	ALIGN_3

.L57:
	testl	$2, N
	jle	.L58

	movaps	-32 * SIZE(Y), %xmm4
	movlps	-32 * SIZE(X), %xmm6
	movhps	-30 * SIZE(X), %xmm6

	PSHUFD2($0xb1, %xmm4, %xmm3)
	mulps	%xmm6, %xmm4
	addps	%xmm4, %xmm0
	mulps	%xmm6, %xmm3
	addps	%xmm3, %xmm1

	addl	$4 * SIZE, X
	addl	$4 * SIZE, Y
	ALIGN_3

.L58:
	testl	$1, N
	jle	.L98

#ifdef movsd
	xorps	%xmm4, %xmm4
#endif
	movsd	-32 * SIZE(Y), %xmm4
#ifdef movsd
	xorps	%xmm6, %xmm6
#endif
	movsd	-32 * SIZE(X), %xmm6

	PSHUFD2($0xb1, %xmm4, %xmm3)
	mulps	%xmm6, %xmm4
	addps	%xmm4, %xmm0
	mulps	%xmm6, %xmm3
	addps	%xmm3, %xmm1
	jmp	.L98
	ALIGN_3
#endif

.L70:
	testl	$2 * SIZE, Y
	je	.L70x

#ifdef movsd
	xorps	%xmm4, %xmm4
#endif
	movsd	-32 * SIZE(X), %xmm4
	addl	$2 * SIZE, X
#ifdef movsd
	xorps	%xmm1, %xmm1
#endif
	movsd	-32 * SIZE(Y), %xmm1
	addl	$2 * SIZE, Y

	PSHUFD2($0xb1, %xmm1, %xmm0)
	shufps	$0xb1, %xmm4, %xmm4

	mulps	%xmm4,  %xmm0
	mulps	%xmm4,  %xmm1
	decl	N
	ALIGN_3

.L70x:
	testl	$2 * SIZE, X
	jne	.L80

	movaps	-33 * SIZE(X), %xmm4
	addl	$3 * SIZE, X
	movaps	-33 * SIZE(Y), %xmm6
	addl	$3 * SIZE, Y

	movl	N,  %eax
	sarl	$4, %eax
	jle	.L75

	movaps	-32 * SIZE(X), %xmm5
	movaps	-32 * SIZE(Y), %xmm7

	decl	%eax
	jle	.L72
	ALIGN_3

.L71:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movss	%xmm7, %xmm6
	PSHUFD2($0x1b, %xmm6, %xmm3)
	movss	%xmm5, %xmm4
	mulps	%xmm4, %xmm6
	addps	%xmm6, %xmm0
	movaps	-28 * SIZE(Y), %xmm6
	mulps	%xmm4, %xmm3
	movaps	-28 * SIZE(X), %xmm4
	addps	%xmm3, %xmm1

	movss	%xmm6, %xmm7
	PSHUFD2($0x1b, %xmm7, %xmm3)
	movss	%xmm4, %xmm5
	mulps	%xmm5, %xmm7
	addps	%xmm7, %xmm0
	movaps	-24 * SIZE(Y), %xmm7
	mulps	%xmm5, %xmm3
	movaps	-24 * SIZE(X), %xmm5
	addps	%xmm3, %xmm1

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	movss	%xmm7, %xmm6
	PSHUFD2($0x1b, %xmm6, %xmm3)
	movss	%xmm5, %xmm4
	mulps	%xmm4, %xmm6
	addps	%xmm6, %xmm0
	movaps	-20 * SIZE(Y), %xmm6
	mulps	%xmm4, %xmm3
	movaps	-20 * SIZE(X), %xmm4
	addps	%xmm3, %xmm1

	movss	%xmm6, %xmm7
	PSHUFD2($0x1b, %xmm7, %xmm3)
	movss	%xmm4, %xmm5
	mulps	%xmm5, %xmm7
	addps	%xmm7, %xmm0
	movaps	-16 * SIZE(Y), %xmm7
	mulps	%xmm5, %xmm3
	movaps	-16 * SIZE(X), %xmm5
	addps	%xmm3, %xmm1

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	movss	%xmm7, %xmm6
	PSHUFD2($0x1b, %xmm6, %xmm3)
	movss	%xmm5, %xmm4
	mulps	%xmm4, %xmm6
	addps	%xmm6, %xmm0
	movaps	-12 * SIZE(Y), %xmm6
	mulps	%xmm4, %xmm3
	movaps	-12 * SIZE(X), %xmm4
	addps	%xmm3, %xmm1

	movss	%xmm6, %xmm7
	PSHUFD2($0x1b, %xmm7, %xmm3)
	movss	%xmm4, %xmm5
	mulps	%xmm5, %xmm7
	addps	%xmm7, %xmm0
	movaps	 -8 * SIZE(Y), %xmm7
	mulps	%xmm5, %xmm3
	movaps	 -8 * SIZE(X), %xmm5
	addps	%xmm3, %xmm1

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	movss	%xmm7, %xmm6
	PSHUFD2($0x1b, %xmm6, %xmm3)
	movss	%xmm5, %xmm4
	mulps	%xmm4, %xmm6
	addps	%xmm6, %xmm0
	movaps	 -4 * SIZE(Y), %xmm6
	mulps	%xmm4, %xmm3
	movaps	 -4 * SIZE(X), %xmm4
	addps	%xmm3, %xmm1

	movss	%xmm6, %xmm7
	PSHUFD2($0x1b, %xmm7, %xmm3)
	movss	%xmm4, %xmm5
	mulps	%xmm5, %xmm7
	addps	%xmm7, %xmm0
	movaps	  0 * SIZE(Y), %xmm7
	mulps	%xmm5, %xmm3
	movaps	  0 * SIZE(X), %xmm5
	addps	%xmm3, %xmm1

	subl	$-32 * SIZE, X
	subl	$-32 * SIZE, Y

	decl	%eax
	jg	.L71
	ALIGN_3

.L72:
	movss	%xmm7, %xmm6
	PSHUFD2($0x1b, %xmm6, %xmm3)
	movss	%xmm5, %xmm4
	mulps	%xmm4, %xmm6
	addps	%xmm6, %xmm0
	movaps	-28 * SIZE(Y), %xmm6
	mulps	%xmm4, %xmm3
	movaps	-28 * SIZE(X), %xmm4
	addps	%xmm3, %xmm1

	movss	%xmm6, %xmm7
	PSHUFD2($0x1b, %xmm7, %xmm3)
	movss	%xmm4, %xmm5
	mulps	%xmm5, %xmm7
	addps	%xmm7, %xmm0
	movaps	-24 * SIZE(Y), %xmm7
	mulps	%xmm5, %xmm3
	movaps	-24 * SIZE(X), %xmm5
	addps	%xmm3, %xmm1

	movss	%xmm7, %xmm6
	PSHUFD2($0x1b, %xmm6, %xmm3)
	movss	%xmm5, %xmm4
	mulps	%xmm4, %xmm6
	addps	%xmm6, %xmm0
	movaps	-20 * SIZE(Y), %xmm6
	mulps	%xmm4, %xmm3
	movaps	-20 * SIZE(X), %xmm4
	addps	%xmm3, %xmm1

	movss	%xmm6, %xmm7
	PSHUFD2($0x1b, %xmm7, %xmm3)
	movss	%xmm4, %xmm5
	mulps	%xmm5, %xmm7
	addps	%xmm7, %xmm0
	movaps	-16 * SIZE(Y), %xmm7
	mulps	%xmm5, %xmm3
	movaps	-16 * SIZE(X), %xmm5
	addps	%xmm3, %xmm1

	movss	%xmm7, %xmm6
	PSHUFD2($0x1b, %xmm6, %xmm3)
	movss	%xmm5, %xmm4
	mulps	%xmm4, %xmm6
	addps	%xmm6, %xmm0
	movaps	-12 * SIZE(Y), %xmm6
	mulps	%xmm4, %xmm3
	movaps	-12 * SIZE(X), %xmm4
	addps	%xmm3, %xmm1

	movss	%xmm6, %xmm7
	PSHUFD2($0x1b, %xmm7, %xmm3)
	movss	%xmm4, %xmm5
	mulps	%xmm5, %xmm7
	addps	%xmm7, %xmm0
	movaps	 -8 * SIZE(Y), %xmm7
	mulps	%xmm5, %xmm3
	movaps	 -8 * SIZE(X), %xmm5
	addps	%xmm3, %xmm1

	movss	%xmm7, %xmm6
	PSHUFD2($0x1b, %xmm6, %xmm3)
	movss	%xmm5, %xmm4
	mulps	%xmm4, %xmm6
	addps	%xmm6, %xmm0
	movaps	 -4 * SIZE(Y), %xmm6
	mulps	%xmm4, %xmm3
	movaps	 -4 * SIZE(X), %xmm4
	addps	%xmm3, %xmm1

	movss	%xmm6, %xmm7
	PSHUFD2($0x1b, %xmm7, %xmm3)
	movss	%xmm4, %xmm5
	mulps	%xmm5, %xmm7
	addps	%xmm7, %xmm0
	mulps	%xmm5, %xmm3
	addps	%xmm3, %xmm1

	subl	$-32 * SIZE, X
	subl	$-32 * SIZE, Y
	ALIGN_3

.L75:
	testl	$8, N
	jle	.L76

	movaps	-32 * SIZE(X), %xmm5
	movaps	-32 * SIZE(Y), %xmm7

	movss	%xmm7, %xmm6
	PSHUFD2($0x1b, %xmm6, %xmm3)
	movss	%xmm5, %xmm4
	mulps	%xmm4, %xmm6
	addps	%xmm6, %xmm0
	movaps	-28 * SIZE(Y), %xmm6
	mulps	%xmm4, %xmm3
	movaps	-28 * SIZE(X), %xmm4
	addps	%xmm3, %xmm1

	movss	%xmm6, %xmm7
	PSHUFD2($0x1b, %xmm7, %xmm3)
	movss	%xmm4, %xmm5
	mulps	%xmm5, %xmm7
	addps	%xmm7, %xmm0
	movaps	-24 * SIZE(Y), %xmm7
	mulps	%xmm5, %xmm3
	movaps	-24 * SIZE(X), %xmm5
	addps	%xmm3, %xmm1

	movss	%xmm7, %xmm6
	PSHUFD2($0x1b, %xmm6, %xmm3)
	movss	%xmm5, %xmm4
	mulps	%xmm4, %xmm6
	addps	%xmm6, %xmm0
	movaps	-20 * SIZE(Y), %xmm6
	mulps	%xmm4, %xmm3
	movaps	-20 * SIZE(X), %xmm4
	addps	%xmm3, %xmm1

	movss	%xmm6, %xmm7
	PSHUFD2($0x1b, %xmm7, %xmm3)
	movss	%xmm4, %xmm5
	mulps	%xmm5, %xmm7
	addps	%xmm7, %xmm0
	mulps	%xmm5, %xmm3
	addps	%xmm3, %xmm1

	addl	$16 * SIZE, X
	addl	$16 * SIZE, Y
	ALIGN_3

.L76:
	testl	$4, N
	jle	.L77

	movaps	-32 * SIZE(X), %xmm5
	movaps	-32 * SIZE(Y), %xmm7

	movss	%xmm7, %xmm6
	PSHUFD2($0x1b, %xmm6, %xmm3)
	movss	%xmm5, %xmm4
	mulps	%xmm4, %xmm6
	addps	%xmm6, %xmm0
	movaps	-28 * SIZE(Y), %xmm6
	mulps	%xmm4, %xmm3
	movaps	-28 * SIZE(X), %xmm4
	addps	%xmm3, %xmm1

	movss	%xmm6, %xmm7
	PSHUFD2($0x1b, %xmm7, %xmm3)
	movss	%xmm4, %xmm5
	mulps	%xmm5, %xmm7
	addps	%xmm7, %xmm0
	mulps	%xmm5, %xmm3
	addps	%xmm3, %xmm1

	addl	$8 * SIZE, X
	addl	$8 * SIZE, Y
	ALIGN_3

.L77:
	testl	$2, N
	jle	.L78

	movaps	-32 * SIZE(X), %xmm5
	movaps	-32 * SIZE(Y), %xmm7

	movss	%xmm7, %xmm6
	PSHUFD2($0x1b, %xmm6, %xmm3)
	movss	%xmm5, %xmm4
	mulps	%xmm4, %xmm6
	addps	%xmm6, %xmm0
	mulps	%xmm4, %xmm3
	addps	%xmm3, %xmm1

	movaps	%xmm5, %xmm4
	movaps	%xmm7, %xmm6
	ALIGN_3

.L78:
	testl	$1, N
	jle	.L79

	xorps	%xmm5, %xmm5
	movss	%xmm5, %xmm4
	movss	%xmm5, %xmm6

	shufps	$0x24, %xmm4, %xmm4
	PSHUFD2($0x18, %xmm6, %xmm3)
	shufps	$0x24, %xmm6, %xmm6

	mulps	%xmm4, %xmm6
	addps	%xmm6, %xmm0
	mulps	%xmm4, %xmm3
	addps	%xmm3, %xmm1
	ALIGN_3

.L79:
	shufps	$0x39, %xmm0, %xmm0
	shufps	$0x39, %xmm1, %xmm1
	jmp	.L98
	ALIGN_3

.L80:
	movsd	-33 * SIZE(X), %xmm4
	movhps	-31 * SIZE(X), %xmm4
	addl	$3 * SIZE, X
	movaps	-33 * SIZE(Y), %xmm6
	addl	$3 * SIZE, Y

	movl	N,  %eax
	sarl	$4, %eax
	jle	.L85

	movsd	-32 * SIZE(X), %xmm5
	movhps	-30 * SIZE(X), %xmm5
	movaps	-32 * SIZE(Y), %xmm7

	decl	%eax
	jle	.L82
	ALIGN_3

.L81:
#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movss	%xmm7, %xmm6
	PSHUFD2($0x1b, %xmm6, %xmm3)
	movss	%xmm5, %xmm4
	mulps	%xmm4, %xmm6
	addps	%xmm6, %xmm0
	movaps	-28 * SIZE(Y), %xmm6
	mulps	%xmm4, %xmm3
	movsd	-28 * SIZE(X), %xmm4
	movhps	-26 * SIZE(X), %xmm4
	addps	%xmm3, %xmm1

	movss	%xmm6, %xmm7
	PSHUFD2($0x1b, %xmm7, %xmm3)
	movss	%xmm4, %xmm5
	mulps	%xmm5, %xmm7
	addps	%xmm7, %xmm0
	movaps	-24 * SIZE(Y), %xmm7
	mulps	%xmm5, %xmm3
	movsd	-24 * SIZE(X), %xmm5
	movhps	-22 * SIZE(X), %xmm5
	addps	%xmm3, %xmm1

#ifdef PREFETCH
	PREFETCH (PREFETCHSIZE +  0) - PREOFFSET(Y)
#endif

	movss	%xmm7, %xmm6
	PSHUFD2($0x1b, %xmm6, %xmm3)
	movss	%xmm5, %xmm4
	mulps	%xmm4, %xmm6
	addps	%xmm6, %xmm0
	movaps	-20 * SIZE(Y), %xmm6
	mulps	%xmm4, %xmm3
	movsd	-20 * SIZE(X), %xmm4
	movhps	-18 * SIZE(X), %xmm4
	addps	%xmm3, %xmm1

	movss	%xmm6, %xmm7
	PSHUFD2($0x1b, %xmm7, %xmm3)
	movss	%xmm4, %xmm5
	mulps	%xmm5, %xmm7
	addps	%xmm7, %xmm0
	movaps	-16 * SIZE(Y), %xmm7
	mulps	%xmm5, %xmm3
	movsd	-16 * SIZE(X), %xmm5
	movhps	-14 * SIZE(X), %xmm5
	addps	%xmm3, %xmm1

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(X)
#endif

	movss	%xmm7, %xmm6
	PSHUFD2($0x1b, %xmm6, %xmm3)
	movss	%xmm5, %xmm4
	mulps	%xmm4, %xmm6
	addps	%xmm6, %xmm0
	movaps	-12 * SIZE(Y), %xmm6
	mulps	%xmm4, %xmm3
	movsd	-12 * SIZE(X), %xmm4
	movhps	-10 * SIZE(X), %xmm4
	addps	%xmm3, %xmm1

	movss	%xmm6, %xmm7
	PSHUFD2($0x1b, %xmm7, %xmm3)
	movss	%xmm4, %xmm5
	mulps	%xmm5, %xmm7
	addps	%xmm7, %xmm0
	movaps	 -8 * SIZE(Y), %xmm7
	mulps	%xmm5, %xmm3
	movsd	 -8 * SIZE(X), %xmm5
	movhps	 -6 * SIZE(X), %xmm5
	addps	%xmm3, %xmm1

#if defined(PREFETCH) && !defined(FETCH128)
	PREFETCH (PREFETCHSIZE +  64) - PREOFFSET(Y)
#endif

	movss	%xmm7, %xmm6
	PSHUFD2($0x1b, %xmm6, %xmm3)
	movss	%xmm5, %xmm4
	mulps	%xmm4, %xmm6
	addps	%xmm6, %xmm0
	movaps	 -4 * SIZE(Y), %xmm6
	mulps	%xmm4, %xmm3
	movsd	 -4 * SIZE(X), %xmm4
	movhps	 -2 * SIZE(X), %xmm4
	addps	%xmm3, %xmm1

	movss	%xmm6, %xmm7
	PSHUFD2($0x1b, %xmm7, %xmm3)
	movss	%xmm4, %xmm5
	mulps	%xmm5, %xmm7
	addps	%xmm7, %xmm0
	movaps	  0 * SIZE(Y), %xmm7
	mulps	%xmm5, %xmm3
	movsd	  0 * SIZE(X), %xmm5
	movhps	  2 * SIZE(X), %xmm5
	addps	%xmm3, %xmm1

	subl	$-32 * SIZE, X
	subl	$-32 * SIZE, Y

	decl	%eax
	jg	.L81
	ALIGN_3

.L82:
	movss	%xmm7, %xmm6
	PSHUFD2($0x1b, %xmm6, %xmm3)
	movss	%xmm5, %xmm4
	mulps	%xmm4, %xmm6
	addps	%xmm6, %xmm0
	movaps	-28 * SIZE(Y), %xmm6
	mulps	%xmm4, %xmm3
	movsd	-28 * SIZE(X), %xmm4
	movhps	-26 * SIZE(X), %xmm4
	addps	%xmm3, %xmm1

	movss	%xmm6, %xmm7
	PSHUFD2($0x1b, %xmm7, %xmm3)
	movss	%xmm4, %xmm5
	mulps	%xmm5, %xmm7
	addps	%xmm7, %xmm0
	movaps	-24 * SIZE(Y), %xmm7
	mulps	%xmm5, %xmm3
	movsd	-24 * SIZE(X), %xmm5
	movhps	-22 * SIZE(X), %xmm5
	addps	%xmm3, %xmm1

	movss	%xmm7, %xmm6
	PSHUFD2($0x1b, %xmm6, %xmm3)
	movss	%xmm5, %xmm4
	mulps	%xmm4, %xmm6
	addps	%xmm6, %xmm0
	movaps	-20 * SIZE(Y), %xmm6
	mulps	%xmm4, %xmm3
	movsd	-20 * SIZE(X), %xmm4
	movhps	-18 * SIZE(X), %xmm4
	addps	%xmm3, %xmm1

	movss	%xmm6, %xmm7
	PSHUFD2($0x1b, %xmm7, %xmm3)
	movss	%xmm4, %xmm5
	mulps	%xmm5, %xmm7
	addps	%xmm7, %xmm0
	movaps	-16 * SIZE(Y), %xmm7
	mulps	%xmm5, %xmm3
	movsd	-16 * SIZE(X), %xmm5
	movhps	-14 * SIZE(X), %xmm5
	addps	%xmm3, %xmm1

	movss	%xmm7, %xmm6
	PSHUFD2($0x1b, %xmm6, %xmm3)
	movss	%xmm5, %xmm4
	mulps	%xmm4, %xmm6
	addps	%xmm6, %xmm0
	movaps	-12 * SIZE(Y), %xmm6
	mulps	%xmm4, %xmm3
	movsd	-12 * SIZE(X), %xmm4
	movhps	-10 * SIZE(X), %xmm4
	addps	%xmm3, %xmm1

	movss	%xmm6, %xmm7
	PSHUFD2($0x1b, %xmm7, %xmm3)
	movss	%xmm4, %xmm5
	mulps	%xmm5, %xmm7
	addps	%xmm7, %xmm0
	movaps	 -8 * SIZE(Y), %xmm7
	mulps	%xmm5, %xmm3
	movsd	 -8 * SIZE(X), %xmm5
	movhps	 -6 * SIZE(X), %xmm5
	addps	%xmm3, %xmm1

	movss	%xmm7, %xmm6
	PSHUFD2($0x1b, %xmm6, %xmm3)
	movss	%xmm5, %xmm4
	mulps	%xmm4, %xmm6
	addps	%xmm6, %xmm0
	movaps	 -4 * SIZE(Y), %xmm6
	mulps	%xmm4, %xmm3
	movsd	 -4 * SIZE(X), %xmm4
	movhps	 -2 * SIZE(X), %xmm4
	addps	%xmm3, %xmm1

	movss	%xmm6, %xmm7
	PSHUFD2($0x1b, %xmm7, %xmm3)
	movss	%xmm4, %xmm5
	mulps	%xmm5, %xmm7
	addps	%xmm7, %xmm0
	mulps	%xmm5, %xmm3
	addps	%xmm3, %xmm1

	subl	$-32 * SIZE, X
	subl	$-32 * SIZE, Y
	ALIGN_3

.L85:
	testl	$8, N
	jle	.L86

	movsd	-32 * SIZE(X), %xmm5
	movhps	-30 * SIZE(X), %xmm5
	movaps	-32 * SIZE(Y), %xmm7

	movss	%xmm7, %xmm6
	PSHUFD2($0x1b, %xmm6, %xmm3)
	movss	%xmm5, %xmm4
	mulps	%xmm4, %xmm6
	addps	%xmm6, %xmm0
	movaps	-28 * SIZE(Y), %xmm6
	mulps	%xmm4, %xmm3
	movsd	-28 * SIZE(X), %xmm4
	movhps	-26 * SIZE(X), %xmm4
	addps	%xmm3, %xmm1

	movss	%xmm6, %xmm7
	PSHUFD2($0x1b, %xmm7, %xmm3)
	movss	%xmm4, %xmm5
	mulps	%xmm5, %xmm7
	addps	%xmm7, %xmm0
	movaps	-24 * SIZE(Y), %xmm7
	mulps	%xmm5, %xmm3
	movsd	-24 * SIZE(X), %xmm5
	movhps	-22 * SIZE(X), %xmm5
	addps	%xmm3, %xmm1

	movss	%xmm7, %xmm6
	PSHUFD2($0x1b, %xmm6, %xmm3)
	movss	%xmm5, %xmm4
	mulps	%xmm4, %xmm6
	addps	%xmm6, %xmm0
	movaps	-20 * SIZE(Y), %xmm6
	mulps	%xmm4, %xmm3
	movsd	-20 * SIZE(X), %xmm4
	movhps	-18 * SIZE(X), %xmm4
	addps	%xmm3, %xmm1

	movss	%xmm6, %xmm7
	PSHUFD2($0x1b, %xmm7, %xmm3)
	movss	%xmm4, %xmm5
	mulps	%xmm5, %xmm7
	addps	%xmm7, %xmm0
	mulps	%xmm5, %xmm3
	addps	%xmm3, %xmm1

	addl	$16 * SIZE, X
	addl	$16 * SIZE, Y
	ALIGN_3

.L86:
	testl	$4, N
	jle	.L87

	movsd	-32 * SIZE(X), %xmm5
	movhps	-30 * SIZE(X), %xmm5
	movaps	-32 * SIZE(Y), %xmm7

	movss	%xmm7, %xmm6
	PSHUFD2($0x1b, %xmm6, %xmm3)
	movss	%xmm5, %xmm4
	mulps	%xmm4, %xmm6
	addps	%xmm6, %xmm0
	movaps	-28 * SIZE(Y), %xmm6
	mulps	%xmm4, %xmm3
	movsd	-28 * SIZE(X), %xmm4
	movhps	-26 * SIZE(X), %xmm4
	addps	%xmm3, %xmm1

	movss	%xmm6, %xmm7
	PSHUFD2($0x1b, %xmm7, %xmm3)
	movss	%xmm4, %xmm5
	mulps	%xmm5, %xmm7
	addps	%xmm7, %xmm0
	mulps	%xmm5, %xmm3
	addps	%xmm3, %xmm1

	addl	$8 * SIZE, X
	addl	$8 * SIZE, Y
	ALIGN_3

.L87:
	testl	$2, N
	jle	.L88

	movsd	-32 * SIZE(X), %xmm5
	movhps	-30 * SIZE(X), %xmm5
	movaps	-32 * SIZE(Y), %xmm7

	movss	%xmm7, %xmm6
	PSHUFD2($0x1b, %xmm6, %xmm3)
	movss	%xmm5, %xmm4
	mulps	%xmm4, %xmm6
	addps	%xmm6, %xmm0
	mulps	%xmm4, %xmm3
	addps	%xmm3, %xmm1

	movaps	%xmm5, %xmm4
	movaps	%xmm7, %xmm6
	ALIGN_3

.L88:
	testl	$1, N
	jle	.L89

	xorps	%xmm5, %xmm5
	movss	%xmm5, %xmm4
	movss	%xmm5, %xmm6

	shufps	$0x24, %xmm4, %xmm4
	PSHUFD2($0x18, %xmm6, %xmm3)
	shufps	$0x24, %xmm6, %xmm6

	mulps	%xmm4, %xmm6
	addps	%xmm6, %xmm0
	mulps	%xmm4, %xmm3
	addps	%xmm3, %xmm1
	ALIGN_3

.L89:
	shufps	$0x39, %xmm0, %xmm0
	shufps	$0x39, %xmm1, %xmm1
	jmp	.L98
	ALIGN_3

.L200:
	movl	N,  %eax
	sarl	$4, %eax
	jle	.L205

	movsd	(X), %xmm4
	addl	INCX, X
	movhps	(X), %xmm4
	addl	INCX, X
	movsd	(Y), %xmm6
	addl	INCY, Y
	movhps	(Y), %xmm6
	addl	INCY, Y

	movsd	(X), %xmm5
	addl	INCX, X
	movhps	(X), %xmm5
	addl	INCX, X
	movsd	(Y), %xmm7
	addl	INCY, Y
	movhps	(Y), %xmm7
	addl	INCY, Y

	decl	%eax
	jle	.L204
	ALIGN_3

.L203:
	PSHUFD2($0xb1, %xmm6, %xmm3)
	mulps	%xmm4, %xmm6
	addps	%xmm6, %xmm0
	movsd	(Y), %xmm6
	addl	INCY, Y
	movhps	(Y), %xmm6
	addl	INCY, Y
	mulps	%xmm4, %xmm3
	movsd	(X), %xmm4
	addl	INCX, X
	movhps	(X), %xmm4
	addl	INCX, X
	addps	%xmm3, %xmm1

	PSHUFD2($0xb1, %xmm7, %xmm3)
	mulps	%xmm5, %xmm7
	addps	%xmm7, %xmm0
	movsd	(Y), %xmm7
	addl	INCY, Y
	movhps	(Y), %xmm7
	addl	INCY, Y

	mulps	%xmm5, %xmm3
	movsd	(X), %xmm5
	addl	INCX, X
	movhps	(X), %xmm5
	addl	INCX, X
	addps	%xmm3, %xmm1

	PSHUFD2($0xb1, %xmm6, %xmm3)
	mulps	%xmm4, %xmm6
	addps	%xmm6, %xmm0
	movsd	(Y), %xmm6
	addl	INCY, Y
	movhps	(Y), %xmm6
	addl	INCY, Y
	mulps	%xmm4, %xmm3
	movsd	(X), %xmm4
	addl	INCX, X
	movhps	(X), %xmm4
	addl	INCX, X
	addps	%xmm3, %xmm1

	PSHUFD2($0xb1, %xmm7, %xmm3)
	mulps	%xmm5, %xmm7
	addps	%xmm7, %xmm0
	movsd	(Y), %xmm7
	addl	INCY, Y
	movhps	(Y), %xmm7
	addl	INCY, Y

	mulps	%xmm5, %xmm3
	movsd	(X), %xmm5
	addl	INCX, X
	movhps	(X), %xmm5
	addl	INCX, X
	addps	%xmm3, %xmm1

	PSHUFD2($0xb1, %xmm6, %xmm3)
	mulps	%xmm4, %xmm6
	addps	%xmm6, %xmm0
	movsd	(Y), %xmm6
	addl	INCY, Y
	movhps	(Y), %xmm6
	addl	INCY, Y
	mulps	%xmm4, %xmm3
	movsd	(X), %xmm4
	addl	INCX, X
	movhps	(X), %xmm4
	addl	INCX, X
	addps	%xmm3, %xmm1

	PSHUFD2($0xb1, %xmm7, %xmm3)
	mulps	%xmm5, %xmm7
	addps	%xmm7, %xmm0
	movsd	(Y), %xmm7
	addl	INCY, Y
	movhps	(Y), %xmm7
	addl	INCY, Y

	mulps	%xmm5, %xmm3
	movsd	(X), %xmm5
	addl	INCX, X
	movhps	(X), %xmm5
	addl	INCX, X
	addps	%xmm3, %xmm1

	PSHUFD2($0xb1, %xmm6, %xmm3)
	mulps	%xmm4, %xmm6
	addps	%xmm6, %xmm0
	movsd	(Y), %xmm6
	addl	INCY, Y
	movhps	(Y), %xmm6
	addl	INCY, Y
	mulps	%xmm4, %xmm3
	movsd	(X), %xmm4
	addl	INCX, X
	movhps	(X), %xmm4
	addl	INCX, X
	addps	%xmm3, %xmm1

	PSHUFD2($0xb1, %xmm7, %xmm3)
	mulps	%xmm5, %xmm7
	addps	%xmm7, %xmm0
	movsd	(Y), %xmm7
	addl	INCY, Y
	movhps	(Y), %xmm7
	addl	INCY, Y
	mulps	%xmm5, %xmm3
	movsd	(X), %xmm5
	addl	INCX, X
	movhps	(X), %xmm5
	addl	INCX, X
	addps	%xmm3, %xmm1

	decl	%eax
	jg	.L203
	ALIGN_3

.L204:
	PSHUFD2($0xb1, %xmm6, %xmm3)
	mulps	%xmm4, %xmm6
	addps	%xmm6, %xmm0
	movsd	(Y), %xmm6
	addl	INCY, Y
	movhps	(Y), %xmm6
	addl	INCY, Y
	mulps	%xmm4, %xmm3
	movsd	(X), %xmm4
	addl	INCX, X
	movhps	(X), %xmm4
	addl	INCX, X
	addps	%xmm3, %xmm1

	PSHUFD2($0xb1, %xmm7, %xmm3)
	mulps	%xmm5, %xmm7
	addps	%xmm7, %xmm0
	movsd	(Y), %xmm7
	addl	INCY, Y
	movhps	(Y), %xmm7
	addl	INCY, Y

	mulps	%xmm5, %xmm3
	movsd	(X), %xmm5
	addl	INCX, X
	movhps	(X), %xmm5
	addl	INCX, X
	addps	%xmm3, %xmm1

	PSHUFD2($0xb1, %xmm6, %xmm3)
	mulps	%xmm4, %xmm6
	addps	%xmm6, %xmm0
	movsd	(Y), %xmm6
	addl	INCY, Y
	movhps	(Y), %xmm6
	addl	INCY, Y
	mulps	%xmm4, %xmm3
	movsd	(X), %xmm4
	addl	INCX, X
	movhps	(X), %xmm4
	addl	INCX, X
	addps	%xmm3, %xmm1

	PSHUFD2($0xb1, %xmm7, %xmm3)
	mulps	%xmm5, %xmm7
	addps	%xmm7, %xmm0
	movsd	(Y), %xmm7
	addl	INCY, Y
	movhps	(Y), %xmm7
	addl	INCY, Y

	mulps	%xmm5, %xmm3
	movsd	(X), %xmm5
	addl	INCX, X
	movhps	(X), %xmm5
	addl	INCX, X
	addps	%xmm3, %xmm1

	PSHUFD2($0xb1, %xmm6, %xmm3)
	mulps	%xmm4, %xmm6
	addps	%xmm6, %xmm0
	movsd	(Y), %xmm6
	addl	INCY, Y
	movhps	(Y), %xmm6
	addl	INCY, Y
	mulps	%xmm4, %xmm3
	movsd	(X), %xmm4
	addl	INCX, X
	movhps	(X), %xmm4
	addl	INCX, X
	addps	%xmm3, %xmm1

	PSHUFD2($0xb1, %xmm7, %xmm3)
	mulps	%xmm5, %xmm7
	addps	%xmm7, %xmm0
	movsd	(Y), %xmm7
	addl	INCY, Y
	movhps	(Y), %xmm7
	addl	INCY, Y

	mulps	%xmm5, %xmm3
	movsd	(X), %xmm5
	addl	INCX, X
	movhps	(X), %xmm5
	addl	INCX, X
	addps	%xmm3, %xmm1

	PSHUFD2($0xb1, %xmm6, %xmm3)
	mulps	%xmm4, %xmm6
	addps	%xmm6, %xmm0
	mulps	%xmm4, %xmm3
	addps	%xmm3, %xmm1

	PSHUFD2($0xb1, %xmm7, %xmm3)
	mulps	%xmm5, %xmm7
	addps	%xmm7, %xmm0
	mulps	%xmm5, %xmm3
	addps	%xmm3, %xmm1
	ALIGN_3

.L205:
	testl	$8, N
	jle	.L206

	movsd	(X), %xmm4
	addl	INCX, X
	movhps	(X), %xmm4
	addl	INCX, X
	movsd	(Y), %xmm6
	addl	INCY, Y
	movhps	(Y), %xmm6
	addl	INCY, Y

	movsd	(X), %xmm5
	addl	INCX, X
	movhps	(X), %xmm5
	addl	INCX, X
	movsd	(Y), %xmm7
	addl	INCY, Y
	movhps	(Y), %xmm7
	addl	INCY, Y

	PSHUFD2($0xb1, %xmm6, %xmm3)
	mulps	%xmm4, %xmm6
	addps	%xmm6, %xmm0
	movsd	(Y), %xmm6
	addl	INCY, Y
	movhps	(Y), %xmm6
	addl	INCY, Y
	mulps	%xmm4, %xmm3
	movsd	(X), %xmm4
	addl	INCX, X
	movhps	(X), %xmm4
	addl	INCX, X
	addps	%xmm3, %xmm1

	PSHUFD2($0xb1, %xmm7, %xmm3)
	mulps	%xmm5, %xmm7
	addps	%xmm7, %xmm0
	movsd	(Y), %xmm7
	addl	INCY, Y
	movhps	(Y), %xmm7
	addl	INCY, Y

	mulps	%xmm5, %xmm3
	movsd	(X), %xmm5
	addl	INCX, X
	movhps	(X), %xmm5
	addl	INCX, X
	addps	%xmm3, %xmm1

	PSHUFD2($0xb1, %xmm6, %xmm3)
	mulps	%xmm4, %xmm6
	addps	%xmm6, %xmm0
	mulps	%xmm4, %xmm3
	addps	%xmm3, %xmm1

	PSHUFD2($0xb1, %xmm7, %xmm3)
	mulps	%xmm5, %xmm7
	addps	%xmm7, %xmm0
	mulps	%xmm5, %xmm3
	addps	%xmm3, %xmm1
	ALIGN_3

.L206:
	testl	$4, N
	jle	.L207

	movsd	(X), %xmm4
	addl	INCX, X
	movhps	(X), %xmm4
	addl	INCX, X
	movsd	(Y), %xmm6
	addl	INCY, Y
	movhps	(Y), %xmm6
	addl	INCY, Y

	PSHUFD2($0xb1, %xmm6, %xmm3)
	mulps	%xmm4, %xmm6
	addps	%xmm6, %xmm0
	mulps	%xmm4, %xmm3
	addps	%xmm3, %xmm1

	movsd	(X), %xmm5
	addl	INCX, X
	movhps	(X), %xmm5
	addl	INCX, X
	movsd	(Y), %xmm7
	addl	INCY, Y
	movhps	(Y), %xmm7
	addl	INCY, Y

	PSHUFD2($0xb1, %xmm7, %xmm3)
	mulps	%xmm5, %xmm7
	addps	%xmm7, %xmm0
	mulps	%xmm5, %xmm3
	addps	%xmm3, %xmm1
	ALIGN_3

.L207:
	testl	$2, N
	jle	.L208

	movsd	(X), %xmm4
	addl	INCX, X
	movhps	(X), %xmm4
	addl	INCX, X
	movsd	(Y), %xmm6
	addl	INCY, Y
	movhps	(Y), %xmm6
	addl	INCY, Y

	PSHUFD2($0xb1, %xmm6, %xmm3)
	mulps	%xmm4, %xmm6
	addps	%xmm6, %xmm0
	mulps	%xmm4, %xmm3
	addps	%xmm3, %xmm1
	ALIGN_3

.L208:
	testl	$1, N
	jle	.L98

#ifdef movsd
	xorps	%xmm4, %xmm4
#endif
	movsd	(X), %xmm4
#ifdef movsd
	xorps	%xmm6, %xmm6
#endif
	movsd	(Y), %xmm6

	PSHUFD2($0xb1, %xmm6, %xmm3)
	mulps	%xmm4, %xmm6
	addps	%xmm6, %xmm0
	mulps	%xmm4, %xmm3
	addps	%xmm3, %xmm1
	ALIGN_3

.L98:
	movhlps	%xmm0, %xmm2
	movhlps	%xmm1, %xmm3

	addps	%xmm2, %xmm0
	addps	%xmm3, %xmm1

	PSHUFD2($1, %xmm0, %xmm2)
	PSHUFD2($1, %xmm1, %xmm3)

#ifndef CONJ
	subss	 %xmm2, %xmm0
	addss	 %xmm3, %xmm1
#else
	addss	 %xmm2, %xmm0
	subss	 %xmm3, %xmm1
#endif
	ALIGN_4

.L999:
	subl	$2 * SIZE, %esp
	movss	%xmm0, 0 * SIZE(%esp)
	movss	%xmm1, 1 * SIZE(%esp)
	movl	0 * SIZE(%esp), %eax
	movl	1 * SIZE(%esp), %edx
	addl	$2 * SIZE, %esp

	popl	%ebx
	popl	%esi
	popl	%edi
	ret

	EPILOGUE
