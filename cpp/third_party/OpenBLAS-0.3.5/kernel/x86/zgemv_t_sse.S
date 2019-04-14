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

#ifdef movsd
#undef movsd
#endif

#ifdef PENTIUM3
#ifdef HAVE_SSE
#define PREFETCH	prefetcht0
#define PREFETCHW	prefetcht0
#define PREFETCHSIZE	(16 * 2)
#endif
#define movsd		movlps
#endif

#ifdef PENTIUM4
#define PREFETCH	prefetcht0
#define PREFETCHW	prefetcht0
#define PREFETCHSIZE	(16 * 2)
#endif

#if defined(CORE2) || defined(PENRYN) || defined(DUNNINGTON) || defined(NEHALEM) || defined(SANDYBRIDGE)
#define PREFETCH	prefetcht0
#define PREFETCHW	prefetcht0
#define PREFETCHSIZE	(16 * 7)
#endif

#ifdef OPTERON
#define PREFETCH	prefetchnta
#define PREFETCHW	prefetchw
#define PREFETCHSIZE	(16 * 3)
#define movsd		movlps
#endif

#if defined(BARCELONA) || defined(BULLDOZER)
#define PREFETCH	prefetchnta
#define PREFETCHW	prefetchw
#define PREFETCHSIZE	(16 * 5)
#endif

#ifdef ATOM
#define PREFETCH	prefetchnta
#define PREFETCHW	prefetcht0
#define PREFETCHSIZE	(16 * 6)
#endif

#ifdef NANO
#define PREFETCH	prefetcht0
#define PREFETCHSIZE	(16 * 4)
#endif

#define STACKSIZE	16
#define ARGS	20

#define M		 4 + STACKSIZE+ARGS(%esp)
#define N		 8 + STACKSIZE+ARGS(%esp)
#define ALPHA_R		16 + STACKSIZE+ARGS(%esp)
#define ALPHA_I		20 + STACKSIZE+ARGS(%esp)
#define A		24 + STACKSIZE+ARGS(%esp)
#define STACK_LDA	28 + STACKSIZE+ARGS(%esp)
#define STACK_X		32 + STACKSIZE+ARGS(%esp)
#define STACK_INCX	36 + STACKSIZE+ARGS(%esp)
#define Y		40 + STACKSIZE+ARGS(%esp)
#define STACK_INCY	44 + STACKSIZE+ARGS(%esp)
#define BUFFER		48 + STACKSIZE+ARGS(%esp)

#define MMM	0+ARGS(%esp)
#define XX	4+ARGS(%esp)
#define AA	8+ARGS(%esp)

#define I	%eax
#define J	%ebx

#define INCX	J
#define INCY	%ecx

#define A1	%esi
#define X	%edx
#define Y1	%edi
#define LDA	%ebp

#undef SUBPS

#ifndef CONJ
#define SUBPS	   addps
#else
#define SUBPS	   subps
#endif

	PROLOGUE

	subl	$ARGS,%esp
	pushl	%ebp
	pushl	%edi
	pushl	%esi
	pushl	%ebx

	PROFCODE

	movl	STACK_X,    X
	movl	X,XX
	movl	A,J
	movl	J,AA		#backup A
	movl	M,J
	movl	J,MMM
.L0t:
	xorl	J,J
	addl	$1,J
	sall	$20,J
	subl	$8,J
	subl	J,MMM		#MMM-=J
	movl	J,M
	jge	.L00t
	ALIGN_4

	movl	MMM,%eax
	addl	J,%eax
	jle	.L999x
	movl	%eax,M

.L00t:
	movl	AA,%eax
	movl	%eax,A

	movl	XX,%eax
	movl	%eax,X

	movl	STACK_LDA,LDA
	movl	STACK_INCX, INCX
	movl	STACK_INCY, INCY

	sall	$ZBASE_SHIFT, INCX
	sall	$ZBASE_SHIFT, LDA
	sall	$ZBASE_SHIFT, INCY

	subl	$-32 * SIZE, A

	cmpl	$0, N
	jle	.L999
	cmpl	$0, M
	jle	.L999

	movl	BUFFER, Y1

	movl	M,  I
	sarl	$2, I
	jle	.L05
	ALIGN_4

.L02:
	movsd	(X), %xmm0
	addl	INCX, X
	movhps	(X), %xmm0
	addl	INCX, X

	movsd	(X), %xmm1
	addl	INCX, X
	movhps	(X), %xmm1
	addl	INCX, X

	movsd	(X), %xmm2
	addl	INCX, X
	movhps	(X), %xmm2
	addl	INCX, X

	movsd	(X), %xmm3
	addl	INCX, X
	movhps	(X), %xmm3
	addl	INCX, X

	movaps	%xmm0,  0 * SIZE(Y1)
	movaps	%xmm1,  4 * SIZE(Y1)
	movaps	%xmm2,  8 * SIZE(Y1)
	movaps	%xmm3, 12 * SIZE(Y1)

	addl	$16 * SIZE, Y1
	decl	I
	jg	.L02
	ALIGN_4

.L05:
	movl	M,  I
	andl	$3, I
	jle	.L10
	ALIGN_2

.L06:
	movsd	(X), %xmm0
	addl	INCX, X

	movlps	%xmm0, (Y1)
	addl	$2 * SIZE, Y1
	decl	I
	jg	.L06
	ALIGN_4

.L10:
	movl	Y, Y1

	movl	N,  J
	ALIGN_3

.L11:
	movl	BUFFER, X
	addl	$32 * SIZE, X

	movl	A, A1
	addl	LDA, A

	xorps	%xmm0, %xmm0
	xorps	%xmm1, %xmm1

	movaps	-32 * SIZE(X), %xmm2
	movaps	-28 * SIZE(X), %xmm3

	movl	M,   I
	sarl	$3,  I
	jle	.L15

	movsd	-32 * SIZE(A1), %xmm4
	movhps	-30 * SIZE(A1), %xmm4
	movsd	-28 * SIZE(A1), %xmm6
	movhps	-26 * SIZE(A1), %xmm6

	decl	I
	jle	.L13
	ALIGN_4

.L12:
#ifdef PREFETCH
	PREFETCH PREFETCHSIZE * SIZE(A1)
#endif

#ifdef HAVE_SSE2
	pshufd	$0xb1, %xmm4,  %xmm5
#else
	movaps	%xmm4, %xmm5
	shufps	$0xb1, %xmm5, %xmm5
#endif
	mulps	%xmm2,  %xmm4
	addps	%xmm4,  %xmm0
	mulps	%xmm2,  %xmm5
	movaps	-24 * SIZE(X), %xmm2
	SUBPS	%xmm5,  %xmm1

#ifdef HAVE_SSE2
	pshufd	$0xb1, %xmm6,  %xmm7
#else
	movaps	%xmm6, %xmm7
	shufps	$0xb1, %xmm7, %xmm7
#endif
	mulps	%xmm3,  %xmm6
	addps	%xmm6,  %xmm0
	mulps	%xmm3,  %xmm7
	movaps	-20 * SIZE(X), %xmm3
	SUBPS	%xmm7,  %xmm1

	movsd	-24 * SIZE(A1), %xmm4
	movhps	-22 * SIZE(A1), %xmm4
	movsd	-20 * SIZE(A1), %xmm6
	movhps	-18 * SIZE(A1), %xmm6

#ifdef HAVE_SSE2
	pshufd	$0xb1, %xmm4,  %xmm5
#else
	movaps	%xmm4, %xmm5
	shufps	$0xb1, %xmm5, %xmm5
#endif
	mulps	%xmm2,  %xmm4
	addps	%xmm4,  %xmm0
	mulps	%xmm2,  %xmm5
	movaps	-16 * SIZE(X), %xmm2
	SUBPS	%xmm5,  %xmm1

#ifdef HAVE_SSE2
	pshufd	$0xb1, %xmm6,  %xmm7
#else
	movaps	%xmm6, %xmm7
	shufps	$0xb1, %xmm7, %xmm7
#endif
	mulps	%xmm3,  %xmm6
	addps	%xmm6,  %xmm0
	mulps	%xmm3,  %xmm7
	movaps	-12 * SIZE(X), %xmm3
	SUBPS	%xmm7,  %xmm1

	movsd	-16 * SIZE(A1), %xmm4
	movhps	-14 * SIZE(A1), %xmm4
	movsd	-12 * SIZE(A1), %xmm6
	movhps	-10 * SIZE(A1), %xmm6

	addl	$16 * SIZE, A1
	addl	$16 * SIZE, X

	decl	I
	jg	.L12
	ALIGN_4

.L13:
#ifdef HAVE_SSE2
	pshufd	$0xb1, %xmm4,  %xmm5
#else
	movaps	%xmm4, %xmm5
	shufps	$0xb1, %xmm5, %xmm5
#endif
	mulps	%xmm2,  %xmm4
	addps	%xmm4,  %xmm0
	mulps	%xmm2,  %xmm5
	movaps	-24 * SIZE(X), %xmm2
	SUBPS	%xmm5,  %xmm1

#ifdef HAVE_SSE2
	pshufd	$0xb1, %xmm6,  %xmm7
#else
	movaps	%xmm6, %xmm7
	shufps	$0xb1, %xmm7, %xmm7
#endif
	mulps	%xmm3,  %xmm6
	addps	%xmm6,  %xmm0
	mulps	%xmm3,  %xmm7
	movaps	-20 * SIZE(X), %xmm3
	SUBPS	%xmm7,  %xmm1

	movsd	-24 * SIZE(A1), %xmm4
	movhps	-22 * SIZE(A1), %xmm4
	movsd	-20 * SIZE(A1), %xmm6
	movhps	-18 * SIZE(A1), %xmm6

#ifdef HAVE_SSE2
	pshufd	$0xb1, %xmm4,  %xmm5
#else
	movaps	%xmm4, %xmm5
	shufps	$0xb1, %xmm5, %xmm5
#endif
	mulps	%xmm2,  %xmm4
	addps	%xmm4,  %xmm0
	mulps	%xmm2,  %xmm5
	movaps	-16 * SIZE(X), %xmm2
	SUBPS	%xmm5,  %xmm1

#ifdef HAVE_SSE2
	pshufd	$0xb1, %xmm6,  %xmm7
#else
	movaps	%xmm6, %xmm7
	shufps	$0xb1, %xmm7, %xmm7
#endif
	mulps	%xmm3,  %xmm6
	addps	%xmm6,  %xmm0
	mulps	%xmm3,  %xmm7
	movaps	-12 * SIZE(X), %xmm3
	SUBPS	%xmm7,  %xmm1

	addl	$16 * SIZE, A1
	addl	$16 * SIZE, X
	ALIGN_4

.L15:
	testl	$4, M
	jle	.L17

	movsd	-32 * SIZE(A1), %xmm4
	movhps	-30 * SIZE(A1), %xmm4
	movsd	-28 * SIZE(A1), %xmm6
	movhps	-26 * SIZE(A1), %xmm6

#ifdef HAVE_SSE2
	pshufd	$0xb1, %xmm4,  %xmm5
#else
	movaps	%xmm4, %xmm5
	shufps	$0xb1, %xmm5, %xmm5
#endif
	mulps	%xmm2,  %xmm4
	addps	%xmm4,  %xmm0
	mulps	%xmm2,  %xmm5
	movaps	-24 * SIZE(X), %xmm2
	SUBPS	%xmm5,  %xmm1

#ifdef HAVE_SSE2
	pshufd	$0xb1, %xmm6,  %xmm7
#else
	movaps	%xmm6, %xmm7
	shufps	$0xb1, %xmm7, %xmm7
#endif
	mulps	%xmm3,  %xmm6
	addps	%xmm6,  %xmm0
	mulps	%xmm3,  %xmm7
	movaps	-20 * SIZE(X), %xmm3
	SUBPS	%xmm7,  %xmm1

	addl	$8 * SIZE, A1
	addl	$8 * SIZE, X
	ALIGN_4

.L17:
	testl	$2, M
	jle	.L18

	movsd	-32 * SIZE(A1), %xmm4
	movhps	-30 * SIZE(A1), %xmm4

#ifdef HAVE_SSE2
	pshufd	$0xb1, %xmm4,  %xmm5
#else
	movaps	%xmm4, %xmm5
	shufps	$0xb1, %xmm5, %xmm5
#endif
	mulps	%xmm2,  %xmm4
	addps	%xmm4,  %xmm0
	mulps	%xmm2,  %xmm5
	SUBPS	%xmm5,  %xmm1
	movaps %xmm3, %xmm2

	addl	$4 * SIZE, A1
	ALIGN_4

.L18:
	testl	$1, M
	jle	.L19

#ifdef movsd
	xorps	%xmm4, %xmm4
#endif
	movsd	-32 * SIZE(A1), %xmm4
	shufps	$0x44, %xmm2, %xmm2

#ifdef HAVE_SSE2
	pshufd	$0xb1, %xmm4,  %xmm5
#else
	movaps	%xmm4, %xmm5
	shufps	$0xb1, %xmm5, %xmm5
#endif
	mulps	%xmm2,  %xmm4
	addps	%xmm4,  %xmm0
	mulps	%xmm2,  %xmm5
	SUBPS	%xmm5,  %xmm1
	ALIGN_4

.L19:
#ifdef HAVE_SSE2
	pcmpeqb	%xmm5, %xmm5
	psllq	$63,   %xmm5
#else
	subl	$8, %esp
	movl	$0x00000000,  0(%esp)
	movl	$0x80000000,  4(%esp)
	movlps	(%esp), %xmm5
	addl	$8, %esp
	movlhps	%xmm5, %xmm5
#endif

#if (!defined(CONJ) && !defined(XCONJ)) || (defined(CONJ) && defined(XCONJ))
	xorps	%xmm5, %xmm0
#else
	xorps	%xmm5, %xmm1
#endif

#ifdef HAVE_SSE3
	haddps	%xmm1, %xmm0
	haddps	%xmm0, %xmm0
#else
	movaps	%xmm0, %xmm2
	unpcklps %xmm1, %xmm0
	unpckhps %xmm1, %xmm2

	addps	%xmm2, %xmm0

	movhlps	%xmm0, %xmm1

	addps	%xmm1, %xmm0
#endif

#ifdef HAVE_SSE2
	pshufd	$0xb1, %xmm0,  %xmm1
#else
	movaps	%xmm0, %xmm1
	shufps	$0xb1, %xmm1, %xmm1
#endif

	movsd	ALPHA_R, %xmm7
	movlhps	 %xmm7, %xmm7

	mulps	%xmm7, %xmm0
	mulps	%xmm7, %xmm1

	xorps	%xmm5, %xmm0

#ifdef HAVE_SSE3
	haddps	%xmm1, %xmm0
#else
	movaps	%xmm0, %xmm2
	shufps	$0x88, %xmm1, %xmm0
	shufps	$0xdd, %xmm1, %xmm2

	addps	%xmm2, %xmm0
#endif

	movsd	 0 * SIZE(Y1), %xmm4

	shufps	$0xd8, %xmm0, %xmm0
	addps	%xmm4, %xmm0

	movlps	%xmm0,  0 * SIZE(Y1)
	addl	INCY, Y1

	decl	J
	jg	.L11
	ALIGN_4

.L999:
	movl	M,%eax
	sall	$ZBASE_SHIFT, %eax
	addl	%eax,AA
	movl	STACK_INCX,INCX
	imull	INCX,%eax
	addl	%eax,XX
	jmp 	.L0t
	ALIGN_4

.L999x:
	popl	%ebx
	popl	%esi
	popl	%edi
	popl	%ebp

	addl	$ARGS,%esp
	ret

	EPILOGUE
