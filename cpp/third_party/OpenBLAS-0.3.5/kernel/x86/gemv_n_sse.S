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
#define ARGS	16

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
#define YY	4+ARGS(%esp)
#define AA	8+ARGS(%esp)

#define I	%eax
#define J	%ebx

#define INCX	%ecx
#define INCY	J

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

	movl	Y,J
	movl	J,YY				# backup Y
	movl	A,J
	movl	J,AA				# backup A
	movl	M,J
	movl	J,MMM				# backup MM
.L0t:
	xorl	J,J
	addl	$1,J
	sall	$21,J
	subl	J,MMM
	movl	J,M
	jge		.L00t
	ALIGN_4

	movl	MMM,%eax
	addl	J,%eax
	jle		.L999x
	movl	%eax,M

.L00t:
	movl	AA,%eax
	movl	%eax,A

	movl	YY,J
	movl	J,Y

	movl	STACK_LDA,  LDA
	movl	STACK_X,    X
	movl	STACK_INCX, INCX

	leal	(,INCX, SIZE), INCX
	leal	(,LDA,  SIZE), LDA

	subl	$-32 * SIZE, A

	cmpl	$0, N
	jle	.L999
	cmpl	$0, M
	jle	.L999

	movl	BUFFER, Y1

	xorps	%xmm7, %xmm7

	movl	M,   %eax
	addl	$16, %eax
	sarl	$4,  %eax
	ALIGN_3

.L01:
	movaps	%xmm7,  0 * SIZE(Y1)
	movaps	%xmm7,  4 * SIZE(Y1)
	movaps	%xmm7,  8 * SIZE(Y1)
	movaps	%xmm7, 12 * SIZE(Y1)
	subl	$-16 * SIZE, Y1
	decl	%eax
	jg	.L01
	ALIGN_3

.L10:
	movl	N,  J
	sarl	$1, J
	jle	.L20
	ALIGN_3

.L11:
	movl	BUFFER, Y1
	addl	$32 * SIZE, Y1

	movl	A,  A1
	leal	(A1,  LDA, 2), %eax
	movl	%eax, A

	movss	(X), %xmm6
	addl	INCX, X
	movss	(X), %xmm7
	addl	INCX, X

	movss	ALPHA, %xmm0

	mulss	%xmm0, %xmm6
	mulss	%xmm0, %xmm7

	shufps	$0, %xmm6, %xmm6
	shufps	$0, %xmm7, %xmm7
	ALIGN_3

	movl	M,   I
	sarl	$4,  I
	jle	.L15

	movsd	 -32 * SIZE(A1), %xmm2
	movhps	 -30 * SIZE(A1), %xmm2
	movsd	 -28 * SIZE(A1), %xmm3
	movhps	 -26 * SIZE(A1), %xmm3

	movaps	 -32 * SIZE(Y1), %xmm0
	movaps	 -28 * SIZE(Y1), %xmm1

	movsd	 -32 * SIZE(A1, LDA), %xmm4
	movhps	 -30 * SIZE(A1, LDA), %xmm4
	movsd	 -28 * SIZE(A1, LDA), %xmm5
	movhps	 -26 * SIZE(A1, LDA), %xmm5

	decl	 I
	jle	 .L14
	ALIGN_3

.L13:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE + 0) * SIZE(A1)
#endif

	mulps	 %xmm6, %xmm2
	addps	 %xmm2, %xmm0
	movsd	 -24 * SIZE(A1), %xmm2
	movhps	 -22 * SIZE(A1), %xmm2
	mulps	 %xmm6, %xmm3
	addps	 %xmm3, %xmm1
	movsd	 -20 * SIZE(A1), %xmm3
	movhps	 -18 * SIZE(A1), %xmm3

	mulps	 %xmm7, %xmm4
	addps	 %xmm4, %xmm0
	movsd	 -24 * SIZE(A1, LDA), %xmm4
	movhps	 -22 * SIZE(A1, LDA), %xmm4

	movaps	 %xmm0,  -32 * SIZE(Y1)
	movaps	 -24 * SIZE(Y1), %xmm0

	mulps	 %xmm7, %xmm5
	addps	 %xmm5, %xmm1
	movsd	 -20 * SIZE(A1, LDA), %xmm5
	movhps	 -18 * SIZE(A1, LDA), %xmm5

	movaps	 %xmm1,  -28 * SIZE(Y1)
	movaps	 -20 * SIZE(Y1), %xmm1

#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE + 0) * SIZE(A1, LDA)
#endif

	mulps	 %xmm6, %xmm2
	addps	 %xmm2, %xmm0
	movsd	 -16 * SIZE(A1), %xmm2
	movhps	 -14 * SIZE(A1), %xmm2
	mulps	 %xmm6, %xmm3
	addps	 %xmm3, %xmm1
	movsd	 -12 * SIZE(A1), %xmm3
	movhps	 -10 * SIZE(A1), %xmm3

	mulps	 %xmm7, %xmm4
	addps	 %xmm4, %xmm0
	movsd	 -16 * SIZE(A1, LDA), %xmm4
	movhps	 -14 * SIZE(A1, LDA), %xmm4

	movaps	 %xmm0,  -24 * SIZE(Y1)
	movaps	 -16 * SIZE(Y1), %xmm0

	mulps	 %xmm7, %xmm5
	addps	 %xmm5, %xmm1
	movsd	 -12 * SIZE(A1, LDA), %xmm5
	movhps	 -10 * SIZE(A1, LDA), %xmm5

	movaps	 %xmm1,  -20 * SIZE(Y1)
	movaps	 -12 * SIZE(Y1), %xmm1

	subl	 $-16 * SIZE, A1
	subl	 $-16 * SIZE, Y1

	subl	 $1, I
	BRANCH
	jg	.L13
	ALIGN_3

.L14:
	mulps	 %xmm6, %xmm2
	addps	 %xmm2, %xmm0
	movsd	 -24 * SIZE(A1), %xmm2
	movhps	 -22 * SIZE(A1), %xmm2
	mulps	 %xmm6, %xmm3
	addps	 %xmm3, %xmm1
	movsd	 -20 * SIZE(A1), %xmm3
	movhps	 -18 * SIZE(A1), %xmm3

	mulps	 %xmm7, %xmm4
	addps	 %xmm4, %xmm0
	movsd	 -24 * SIZE(A1, LDA), %xmm4
	movhps	 -22 * SIZE(A1, LDA), %xmm4

	movaps	 %xmm0,  -32 * SIZE(Y1)
	movaps	 -24 * SIZE(Y1), %xmm0

	mulps	 %xmm7, %xmm5
	addps	 %xmm5, %xmm1
	movsd	 -20 * SIZE(A1, LDA), %xmm5
	movhps	 -18 * SIZE(A1, LDA), %xmm5

	movaps	 %xmm1,  -28 * SIZE(Y1)
	movaps	 -20 * SIZE(Y1), %xmm1

	mulps	 %xmm6, %xmm2
	addps	 %xmm2, %xmm0
	mulps	 %xmm6, %xmm3
	addps	 %xmm3, %xmm1

	mulps	 %xmm7, %xmm4
	addps	 %xmm4, %xmm0
	movaps	 %xmm0,  -24 * SIZE(Y1)
	mulps	 %xmm7, %xmm5
	addps	 %xmm5, %xmm1
	movaps	 %xmm1,  -20 * SIZE(Y1)

	subl	 $-16 * SIZE, A1
	subl	 $-16 * SIZE, Y1
	ALIGN_3

.L15:
	testl	$8, M
	je	.L16

	movsd	 -32 * SIZE(A1), %xmm2
	movhps	 -30 * SIZE(A1), %xmm2
	movsd	 -28 * SIZE(A1), %xmm3
	movhps	 -26 * SIZE(A1), %xmm3

	movaps	 -32 * SIZE(Y1), %xmm0
	movaps	 -28 * SIZE(Y1), %xmm1

	mulps	 %xmm6, %xmm2
	addps	 %xmm2, %xmm0
	mulps	 %xmm6, %xmm3
	addps	 %xmm3, %xmm1

	movsd	 -32 * SIZE(A1, LDA), %xmm4
	movhps	 -30 * SIZE(A1, LDA), %xmm4
	movsd	 -28 * SIZE(A1, LDA), %xmm5
	movhps	 -26 * SIZE(A1, LDA), %xmm5

	mulps	 %xmm7, %xmm4
	addps	 %xmm4, %xmm0
	mulps	 %xmm7, %xmm5
	addps	 %xmm5, %xmm1

	movaps	 %xmm0,  -32 * SIZE(Y1)
	movaps	 %xmm1,  -28 * SIZE(Y1)

	addl	 $8 * SIZE, A1
	addl	 $8 * SIZE, Y1
	ALIGN_3

.L16:
	testl	$4, M
	je	.L17

	movsd	 -32 * SIZE(A1), %xmm2
	movhps	 -30 * SIZE(A1), %xmm2
	movsd	 -32 * SIZE(A1, LDA), %xmm3
	movhps	 -30 * SIZE(A1, LDA), %xmm3

	movaps	 -32 * SIZE(Y1), %xmm0

	mulps	 %xmm6, %xmm2
	addps	 %xmm2, %xmm0
	mulps	 %xmm7, %xmm3
	addps	 %xmm3, %xmm0

	movaps	 %xmm0, -32 * SIZE(Y1)

	addl	 $4 * SIZE, A1
	addl	 $4 * SIZE, Y1
	ALIGN_3

.L17:
	testl	$2, M
	je	.L18

	movsd	 -32 * SIZE(A1), %xmm2
	movsd	 -32 * SIZE(A1, LDA), %xmm3

	movsd	 -32 * SIZE(Y1), %xmm0

	mulps	 %xmm6, %xmm2
	addps	 %xmm2, %xmm0
	mulps	 %xmm7, %xmm3
	addps	 %xmm3, %xmm0

	movlps	 %xmm0, -32 * SIZE(Y1)

	addl	 $2 * SIZE, A1
	addl	 $2 * SIZE, Y1
	ALIGN_3

.L18:
	testl	$1, M
	je	.L19

	movss	 -32 * SIZE(A1), %xmm2
	movss	 -32 * SIZE(A1, LDA), %xmm3

	movss	 -32 * SIZE(Y1), %xmm0

	mulss	 %xmm6, %xmm2
	addss	 %xmm2, %xmm0
	mulss	 %xmm7, %xmm3
	addss	 %xmm3, %xmm0

	movss	 %xmm0, -32 * SIZE(Y1)
	ALIGN_3

.L19:
	decl	J
	jg	.L11
	ALIGN_4

.L20:
	testl	$1, N
	jle	.L990

	movl	BUFFER, Y1
	addl	$32 * SIZE, Y1

	movl	A,  A1

	movss	(X), %xmm6
	addl	INCX, X

	movss	ALPHA, %xmm0

	mulss	%xmm0, %xmm6

	shufps	$0, %xmm6, %xmm6
	ALIGN_3

	movl	M,   I
	sarl	$4,  I
	jle	.L25

	movsd	 -32 * SIZE(A1), %xmm2
	movhps	 -30 * SIZE(A1), %xmm2
	movsd	 -28 * SIZE(A1), %xmm3
	movhps	 -26 * SIZE(A1), %xmm3

	movaps	 -32 * SIZE(Y1), %xmm0
	movaps	 -28 * SIZE(Y1), %xmm1

	decl	 I
	jle	 .L24
	ALIGN_3

.L23:
#ifdef PREFETCH
	PREFETCH	(PREFETCHSIZE + 0) * SIZE(A1)
#endif

	mulps	 %xmm6, %xmm2
	addps	 %xmm2, %xmm0
	movsd	 -24 * SIZE(A1), %xmm2
	movhps	 -22 * SIZE(A1), %xmm2

	movaps	 %xmm0,  -32 * SIZE(Y1)
	movaps	 -24 * SIZE(Y1), %xmm0

	mulps	 %xmm6, %xmm3
	addps	 %xmm3, %xmm1
	movsd	 -20 * SIZE(A1), %xmm3
	movhps	 -18 * SIZE(A1), %xmm3

	movaps	 %xmm1,  -28 * SIZE(Y1)
	movaps	 -20 * SIZE(Y1), %xmm1

	mulps	 %xmm6, %xmm2
	addps	 %xmm2, %xmm0
	movsd	 -16 * SIZE(A1), %xmm2
	movhps	 -14 * SIZE(A1), %xmm2

	movaps	 %xmm0,  -24 * SIZE(Y1)
	movaps	 -16 * SIZE(Y1), %xmm0

	mulps	 %xmm6, %xmm3
	addps	 %xmm3, %xmm1
	movsd	 -12 * SIZE(A1), %xmm3
	movhps	 -10 * SIZE(A1), %xmm3

	movaps	 %xmm1,  -20 * SIZE(Y1)
	movaps	 -12 * SIZE(Y1), %xmm1

	subl	 $-16 * SIZE, A1
	subl	 $-16 * SIZE, Y1

	subl	 $1, I
	BRANCH
	jg	.L23
	ALIGN_3

.L24:
	mulps	 %xmm6, %xmm2
	addps	 %xmm2, %xmm0
	movsd	 -24 * SIZE(A1), %xmm2
	movhps	 -22 * SIZE(A1), %xmm2
	mulps	 %xmm6, %xmm3
	addps	 %xmm3, %xmm1
	movsd	 -20 * SIZE(A1), %xmm3
	movhps	 -18 * SIZE(A1), %xmm3

	movaps	 %xmm0,  -32 * SIZE(Y1)
	movaps	 -24 * SIZE(Y1), %xmm0

	movaps	 %xmm1,  -28 * SIZE(Y1)
	movaps	 -20 * SIZE(Y1), %xmm1

	mulps	 %xmm6, %xmm2
	addps	 %xmm2, %xmm0
	movaps	 %xmm0,  -24 * SIZE(Y1)
	mulps	 %xmm6, %xmm3
	addps	 %xmm3, %xmm1
	movaps	 %xmm1,  -20 * SIZE(Y1)

	subl	 $-16 * SIZE, A1
	subl	 $-16 * SIZE, Y1
	ALIGN_3

.L25:
	testl	$8, M
	je	.L26

	movsd	 -32 * SIZE(A1), %xmm2
	movhps	 -30 * SIZE(A1), %xmm2
	movsd	 -28 * SIZE(A1), %xmm3
	movhps	 -26 * SIZE(A1), %xmm3

	movaps	 -32 * SIZE(Y1), %xmm0
	movaps	 -28 * SIZE(Y1), %xmm1

	mulps	 %xmm6, %xmm2
	addps	 %xmm2, %xmm0
	mulps	 %xmm6, %xmm3
	addps	 %xmm3, %xmm1

	movaps	 %xmm0,  -32 * SIZE(Y1)
	movaps	 %xmm1,  -28 * SIZE(Y1)

	addl	 $8 * SIZE, A1
	addl	 $8 * SIZE, Y1
	ALIGN_3

.L26:
	testl	$4, M
	je	.L27

	movsd	 -32 * SIZE(A1), %xmm2
	movhps	 -30 * SIZE(A1), %xmm2

	movaps	 -32 * SIZE(Y1), %xmm0

	mulps	 %xmm6, %xmm2
	addps	 %xmm2, %xmm0

	movaps	 %xmm0, -32 * SIZE(Y1)

	addl	 $4 * SIZE, A1
	addl	 $4 * SIZE, Y1
	ALIGN_3

.L27:
	testl	$2, M
	je	.L28

	movsd	 -32 * SIZE(A1), %xmm2
	movsd	 -32 * SIZE(Y1), %xmm0

	mulps	 %xmm6, %xmm2
	addps	 %xmm2, %xmm0

	movlps	 %xmm0, -32 * SIZE(Y1)

	addl	 $2 * SIZE, A1
	addl	 $2 * SIZE, Y1
	ALIGN_3

.L28:
	testl	$1, M
	je	.L990

	movss	 -32 * SIZE(A1), %xmm2
	movss	 -32 * SIZE(Y1), %xmm0

	mulss	 %xmm6, %xmm2
	addss	 %xmm2, %xmm0

	movss	 %xmm0, -32 * SIZE(Y1)
	ALIGN_3

.L990:
	movl	Y,   Y1
	movl	BUFFER, X

	movl	STACK_INCY, INCY
	sall	$BASE_SHIFT, INCY

	movl	M,   %eax
	sarl	$2,  %eax
	jle	.L994
	ALIGN_3

.L992:
	movss	(Y1), %xmm0
	addss	0 * SIZE(X), %xmm0
	movss	%xmm0, (Y1)
	addl	INCY, Y1

	movss	(Y1), %xmm0
	addss	1 * SIZE(X), %xmm0
	movss	%xmm0, (Y1)
	addl	INCY, Y1

	movss	(Y1), %xmm0
	addss	2 * SIZE(X), %xmm0
	movss	%xmm0, (Y1)
	addl	INCY, Y1

	movss	(Y1), %xmm0
	addss	3 * SIZE(X), %xmm0
	movss	%xmm0, (Y1)
	addl	INCY, Y1

	addl	$4 * SIZE, X
	decl	%eax
	jg	.L992
	ALIGN_3

.L994:
	testl	$2, M
	jle	.L996

	movss	(Y1), %xmm0
	addss	0 * SIZE(X), %xmm0
	movss	%xmm0, (Y1)
	addl	INCY, Y1

	movss	(Y1), %xmm0
	addss	1 * SIZE(X), %xmm0
	movss	%xmm0, (Y1)
	addl	INCY, Y1

	addl	$2 * SIZE, X
	ALIGN_3

.L996:
	testl	$1, M
	jle	.L999

	movss	(Y1), %xmm0
	addss	0 * SIZE(X), %xmm0
	movss	%xmm0, (Y1)
	ALIGN_3
.L999:
	movl	M,J
	leal	(,J,SIZE),%eax
	addl	%eax,AA
	movl	STACK_INCY,INCY
	imull	INCY,%eax
	addl	%eax,YY
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
