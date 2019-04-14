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
#define PREFETCHSIZE	(16 * 4)
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

#ifdef BARCELONA
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
#define ALPHA		16 + STACKSIZE+ARGS(%esp)
#define A		20 + STACKSIZE+ARGS(%esp)
#define STACK_LDA	24 + STACKSIZE+ARGS(%esp)
#define STACK_X		28 + STACKSIZE+ARGS(%esp)
#define STACK_INCX	32 + STACKSIZE+ARGS(%esp)
#define Y		36 + STACKSIZE+ARGS(%esp)
#define STACK_INCY	40 + STACKSIZE+ARGS(%esp)
#define BUFFER		44 + STACKSIZE+ARGS(%esp)

#define MMM	0+ARGS(%esp)
#define AA	4+ARGS(%esp)
#define XX	8+ARGS(%esp)

#define I	%eax
#define J	%ebx

#define INCX	J
#define INCY	%ecx

#define A1	%esi
#define X	%edx
#define Y1	%edi
#define LDA	%ebp

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
	movl	J,AA				# backup A
    movl	M,J
	movl	J,MMM				# mov M to MMM
.L0t:
	xorl	J,J
	addl	$1,J
	sall    $22,J                           # J=2^24*sizeof(float)=buffer size(16MB)
	subl    $8, J                           # Don't use last 8 float in the buffer.
	subl	J,MMM				# MMM=MMM-J
	movl	J,M
	jge		.L00t
	ALIGN_4

	movl	MMM,%eax
	addl	J,%eax
	jle		.L999x
	movl	%eax,M

.L00t:
	movl	AA,%eax
	movl	%eax,A			 	# mov AA to A

	movl	XX,%eax
	movl	%eax,X

	movl	STACK_LDA, LDA
	movl	STACK_INCX, INCX
	movl	STACK_INCY, INCY

	leal	(,INCX, SIZE), INCX
	leal	(,INCY, SIZE), INCY
	leal	(,LDA,  SIZE), LDA

	subl	$-32 * SIZE, A

	cmpl	$0, N
	jle	.L999
	cmpl	$0, M
	jle	.L999

	movl	BUFFER, Y1

	movl	M,  I
	sarl	$3, I
	jle	.L05
	ALIGN_4

.L02:
	movss	(X), %xmm0
	addl	INCX, X
	movss	(X), %xmm1
	addl	INCX, X

	unpcklps %xmm1, %xmm0

	movss	(X), %xmm2
	addl	INCX, X
	movss	(X), %xmm3
	addl	INCX, X

	unpcklps %xmm3, %xmm2

	movss	(X), %xmm4
	addl	INCX, X
	movss	(X), %xmm5
	addl	INCX, X

	unpcklps %xmm5, %xmm4

	movss	(X), %xmm6
	addl	INCX, X
	movss	(X), %xmm7
	addl	INCX, X

	unpcklps %xmm7, %xmm6

	movlps	%xmm0, 0 * SIZE(Y1)
	movlps	%xmm2, 2 * SIZE(Y1)
	movlps	%xmm4, 4 * SIZE(Y1)
	movlps	%xmm6, 6 * SIZE(Y1)

	addl	$8 * SIZE, Y1
	decl	I
	jg	.L02
	ALIGN_4

.L05:
	movl	M,  I
	andl	$7, I
	jle	.L10
	ALIGN_2

.L06:
	movss	(X), %xmm0
	addl	INCX, X
	movss	%xmm0, 0 * SIZE(Y1)
	addl	$SIZE, Y1
	decl	I
	jg	.L06
	ALIGN_4

//Padding zero to prevent loading the dirty number from buffer.
	movl	M,  I
	movl	$8, J
	andl	$7, I
	xorps	%xmm0, %xmm0
	subl	I, J
	ALIGN_2
.L07:
	movss	%xmm0, 0 * SIZE(Y1)
	addl	$SIZE, Y1
	decl	J
	jg	.L07
	ALIGN_4

.L10:
	movl	Y, Y1

	movl	N,  J
	sarl	$1, J
	jle	.L20
	ALIGN_3

.L11:
	movl	BUFFER, X
	addl	$32 * SIZE, X

	movl	A, A1
	leal	(A1, LDA, 2), %eax
	movl	%eax, A

	xorps	%xmm0, %xmm0
	xorps	%xmm1, %xmm1

	movaps	-32 * SIZE(X), %xmm2
	movaps	-28 * SIZE(X), %xmm3

	movl	M,   I
	sarl	$4,  I
	jle	.L15

	movsd	-32 * SIZE(A1), %xmm4
	movhps	-30 * SIZE(A1), %xmm4
	movsd	-32 * SIZE(A1, LDA), %xmm5
	movhps	-30 * SIZE(A1, LDA), %xmm5

	movsd	-28 * SIZE(A1), %xmm6
	movhps	-26 * SIZE(A1), %xmm6
	movsd	-28 * SIZE(A1, LDA), %xmm7
	movhps	-26 * SIZE(A1, LDA), %xmm7

	decl	I
	jle	.L13
	ALIGN_4

.L12:
#ifdef PREFETCH
	PREFETCH PREFETCHSIZE * SIZE(A1)
#endif

	mulps	%xmm2, %xmm4
	addps	%xmm4, %xmm0
	movsd	-24 * SIZE(A1), %xmm4
	movhps	-22 * SIZE(A1), %xmm4
	mulps	%xmm2, %xmm5
	movaps	-24 * SIZE(X), %xmm2
	addps	%xmm5, %xmm1
	movsd	-24 * SIZE(A1, LDA), %xmm5
	movhps	-22 * SIZE(A1, LDA), %xmm5

	mulps	%xmm3, %xmm6
	addps	%xmm6, %xmm0
	movsd	-20 * SIZE(A1), %xmm6
	movhps	-18 * SIZE(A1), %xmm6
	mulps	%xmm3, %xmm7
	movaps	-20 * SIZE(X), %xmm3
	addps	%xmm7, %xmm1
	movsd	-20 * SIZE(A1, LDA), %xmm7
	movhps	-18 * SIZE(A1, LDA), %xmm7

#ifdef PREFETCH
	PREFETCH PREFETCHSIZE * SIZE(A1, LDA)
#endif

	mulps	%xmm2, %xmm4
	addps	%xmm4, %xmm0
	movsd	-16 * SIZE(A1), %xmm4
	movhps	-14 * SIZE(A1), %xmm4
	mulps	%xmm2, %xmm5
	movaps	-16 * SIZE(X), %xmm2
	addps	%xmm5, %xmm1
	movsd	-16 * SIZE(A1, LDA), %xmm5
	movhps	-14 * SIZE(A1, LDA), %xmm5

	mulps	%xmm3, %xmm6
	addps	%xmm6, %xmm0
	movsd	-12 * SIZE(A1), %xmm6
	movhps	-10 * SIZE(A1), %xmm6
	mulps	%xmm3, %xmm7
	movaps	-12 * SIZE(X), %xmm3
	addps	%xmm7, %xmm1
	movsd	-12 * SIZE(A1, LDA), %xmm7
	movhps	-10 * SIZE(A1, LDA), %xmm7

	addl	$16 * SIZE, A1
	addl	$16 * SIZE, X

	decl	I
	jg	.L12
	ALIGN_4

.L13:
	mulps	%xmm2, %xmm4
	addps	%xmm4, %xmm0
	movsd	-24 * SIZE(A1), %xmm4
	movhps	-22 * SIZE(A1), %xmm4
	mulps	%xmm2, %xmm5
	movaps	-24 * SIZE(X), %xmm2
	addps	%xmm5, %xmm1
	movsd	-24 * SIZE(A1, LDA), %xmm5
	movhps	-22 * SIZE(A1, LDA), %xmm5

	mulps	%xmm3, %xmm6
	addps	%xmm6, %xmm0
	movsd	-20 * SIZE(A1), %xmm6
	movhps	-18 * SIZE(A1), %xmm6
	mulps	%xmm3, %xmm7
	movaps	-20 * SIZE(X), %xmm3
	addps	%xmm7, %xmm1
	movsd	-20 * SIZE(A1, LDA), %xmm7
	movhps	-18 * SIZE(A1, LDA), %xmm7

	mulps	%xmm2, %xmm4
	addps	%xmm4, %xmm0
	mulps	%xmm2, %xmm5
	movaps	-16 * SIZE(X), %xmm2
	addps	%xmm5, %xmm1

	mulps	%xmm3, %xmm6
	addps	%xmm6, %xmm0
	mulps	%xmm3, %xmm7
	movaps	-12 * SIZE(X), %xmm3
	addps	%xmm7, %xmm1

	addl	$16 * SIZE, A1
	addl	$16 * SIZE, X
	ALIGN_4

.L15:
	testl	$8, M
	jle	.L16

	movsd	-32 * SIZE(A1), %xmm4
	movhps	-30 * SIZE(A1), %xmm4
	movsd	-32 * SIZE(A1, LDA), %xmm5
	movhps	-30 * SIZE(A1, LDA), %xmm5

	movsd	-28 * SIZE(A1), %xmm6
	movhps	-26 * SIZE(A1), %xmm6
	movsd	-28 * SIZE(A1, LDA), %xmm7
	movhps	-26 * SIZE(A1, LDA), %xmm7

	mulps	%xmm2, %xmm4
	addps	%xmm4, %xmm0
	mulps	%xmm2, %xmm5
	movaps	-24 * SIZE(X), %xmm2
	addps	%xmm5, %xmm1

	mulps	%xmm3, %xmm6
	addps	%xmm6, %xmm0
	mulps	%xmm3, %xmm7
	movaps	-20 * SIZE(X), %xmm3
	addps	%xmm7, %xmm1

	addl	$8 * SIZE, A1
	addl	$8 * SIZE, X
	ALIGN_4

.L16:
	testl	$4, M
	jle	.L17

	movsd	-32 * SIZE(A1), %xmm4
	movhps	-30 * SIZE(A1), %xmm4

	movsd	-32 * SIZE(A1, LDA), %xmm5
	movhps	-30 * SIZE(A1, LDA), %xmm5

	mulps	%xmm2, %xmm4
	addps	%xmm4, %xmm0
	mulps	%xmm2, %xmm5
	addps	%xmm5, %xmm1
	movaps	%xmm3, %xmm2

	addl	$4 * SIZE, A1
	ALIGN_4

.L17:
	testl	$2, M
	jle	.L18

#ifdef movsd
	xorps	%xmm4, %xmm4
#endif
	movsd	-32 * SIZE(A1), %xmm4

#ifdef movsd
	xorps	%xmm5, %xmm5
#endif
	movsd	-32 * SIZE(A1, LDA), %xmm5

	mulps	%xmm2, %xmm4
	addps	%xmm4, %xmm0
	mulps	%xmm2, %xmm5
	addps	%xmm5, %xmm1
	movhlps	%xmm2, %xmm2

	addl	$2 * SIZE, A1
	ALIGN_4

.L18:
	testl	$1, M
	jle	.L19

	movss	-32 * SIZE(A1), %xmm4
	mulss	%xmm2, %xmm4
	addss	%xmm4, %xmm0
	movss	-32 * SIZE(A1, LDA), %xmm5
	mulss	%xmm2, %xmm5
	addss	%xmm5, %xmm1
	ALIGN_4

.L19:
#ifdef HAVE_SSE3
	haddps	%xmm0, %xmm0
	haddps	%xmm1, %xmm1

	haddps	%xmm0, %xmm0
	haddps	%xmm1, %xmm1
#else
	movhlps	%xmm0, %xmm2
	movhlps	%xmm1, %xmm3

	addps	%xmm2, %xmm0
	addps	%xmm3, %xmm1

	movaps	%xmm0, %xmm2
	shufps	$1, %xmm0, %xmm0
	movaps	%xmm1, %xmm3
	shufps	$1, %xmm1, %xmm1

	addss	%xmm2, %xmm0
	addss	%xmm3, %xmm1
#endif

	movss	ALPHA, %xmm7

	mulss	%xmm7, %xmm0
	mulss	%xmm7, %xmm1

	addss	(Y1), %xmm0
	addss	(Y1, INCY), %xmm1

	movss	%xmm0, (Y1)
	movss	%xmm1, (Y1, INCY)
	leal	(Y1, INCY, 2), Y1

	decl	J
	jg	.L11
	ALIGN_4

.L20:
	testl	$1, N
	jle	.L999

	movl	BUFFER, X
	addl	$32 * SIZE, X

	movl	A, A1

	xorps	%xmm0, %xmm0
	xorps	%xmm1, %xmm1

	movaps	-32 * SIZE(X), %xmm2
	movaps	-28 * SIZE(X), %xmm3

	movl	M,   I
	sarl	$4,  I
	jle	.L25

	movsd	-32 * SIZE(A1), %xmm4
	movhps	-30 * SIZE(A1), %xmm4
	movsd	-28 * SIZE(A1), %xmm6
	movhps	-26 * SIZE(A1), %xmm6

	decl	I
	jle	.L23
	ALIGN_4

.L22:
#ifdef PREFETCH
	PREFETCH PREFETCHSIZE * SIZE(A1)
#endif

	mulps	%xmm2, %xmm4
	movaps	-24 * SIZE(X), %xmm2
	addps	%xmm4, %xmm0
	movsd	-24 * SIZE(A1), %xmm4
	movhps	-22 * SIZE(A1), %xmm4

	mulps	%xmm3, %xmm6
	movaps	-20 * SIZE(X), %xmm3
	addps	%xmm6, %xmm0
	movsd	-20 * SIZE(A1), %xmm6
	movhps	-18 * SIZE(A1), %xmm6

	mulps	%xmm2, %xmm4
	movaps	-16 * SIZE(X), %xmm2
	addps	%xmm4, %xmm0
	movsd	-16 * SIZE(A1), %xmm4
	movhps	-14 * SIZE(A1), %xmm4

	mulps	%xmm3, %xmm6
	movaps	-12 * SIZE(X), %xmm3
	addps	%xmm6, %xmm0
	movsd	-12 * SIZE(A1), %xmm6
	movhps	-10 * SIZE(A1), %xmm6

	addl	$16 * SIZE, A1
	addl	$16 * SIZE, X

	decl	I
	jg	.L22
	ALIGN_4

.L23:
	mulps	%xmm2, %xmm4
	movaps	-24 * SIZE(X), %xmm2
	addps	%xmm4, %xmm0
	movsd	-24 * SIZE(A1), %xmm4
	movhps	-22 * SIZE(A1), %xmm4

	mulps	%xmm3, %xmm6
	movaps	-20 * SIZE(X), %xmm3
	addps	%xmm6, %xmm0
	movsd	-20 * SIZE(A1), %xmm6
	movhps	-18 * SIZE(A1), %xmm6

	mulps	%xmm2, %xmm4
	movaps	-16 * SIZE(X), %xmm2
	addps	%xmm4, %xmm0

	mulps	%xmm3, %xmm6
	movaps	-12 * SIZE(X), %xmm3
	addps	%xmm6, %xmm0

	addl	$16 * SIZE, A1
	addl	$16 * SIZE, X
	ALIGN_4

.L25:
	testl	$8, M
	jle	.L26

	movsd	-32 * SIZE(A1), %xmm4
	movhps	-30 * SIZE(A1), %xmm4
	movsd	-28 * SIZE(A1), %xmm6
	movhps	-26 * SIZE(A1), %xmm6

	mulps	%xmm2, %xmm4
	movaps	-24 * SIZE(X), %xmm2
	addps	%xmm4, %xmm0

	mulps	%xmm3, %xmm6
	movaps	-20 * SIZE(X), %xmm3
	addps	%xmm6, %xmm0

	addl	$8 * SIZE, A1
	addl	$8 * SIZE, X
	ALIGN_4

.L26:
	testl	$4, M
	jle	.L27

	movsd	-32 * SIZE(A1), %xmm4
	movhps	-30 * SIZE(A1), %xmm4

	mulps	%xmm2, %xmm4
	addps	%xmm4, %xmm0
	movaps	%xmm3, %xmm2

	addl	$4 * SIZE, A1
	ALIGN_4

.L27:
	testl	$2, M
	jle	.L28

#ifdef movsd
	xorps	%xmm4, %xmm4
#endif
	movsd	-32 * SIZE(A1), %xmm4

	mulps	%xmm2, %xmm4
	addps	%xmm4, %xmm0
	movhlps	%xmm2, %xmm2

	addl	$2 * SIZE, A1
	ALIGN_4

.L28:
	testl	$1, M
	jle	.L29

	movss	-32 * SIZE(A1), %xmm4
	mulss	%xmm2, %xmm4
	addss	%xmm4, %xmm0
	ALIGN_4

.L29:
#ifdef HAVE_SSE3
	haddps	%xmm0, %xmm0
	haddps	%xmm0, %xmm0
#else
	movhlps	%xmm0, %xmm2

	addps	%xmm2, %xmm0

	movaps	%xmm0, %xmm2
	shufps	$1, %xmm0, %xmm0

	addss	%xmm2, %xmm0
#endif

	movss	ALPHA, %xmm7

	mulss	%xmm7, %xmm0

	addss	(Y1), %xmm0

	movss	%xmm0, (Y1)
	ALIGN_4

.L999:
	movl	M,J
	leal	(,J,SIZE),%eax
	addl	%eax,AA
	movl	STACK_INCX,INCX
	imull	INCX,%eax
	addl	%eax,XX
	jmp		.L0t
	ALIGN_4

.L999x:
	popl	%ebx
	popl	%esi
	popl	%edi
	popl	%ebp

	addl	$ARGS,%esp
	ret

	EPILOGUE
