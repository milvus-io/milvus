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

#define STACK	16
#define ARGS     0

#define STACK_M	 	 4 + STACK + ARGS(%esp)
#define STACK_ALPHA_R	16 + STACK + ARGS(%esp)
#define STACK_ALPHA_I	20 + STACK + ARGS(%esp)
#define STACK_X		24 + STACK + ARGS(%esp)
#define STACK_INCX	28 + STACK + ARGS(%esp)

#define M	%ebx
#define X	%ecx
#define INCX	%edx
#define I	%esi
#define XX	%edi
#define FLAG	%ebp

#if defined(NEHALEM) || defined(PENRYN) || defined(DUNNINGTON) || defined(SANDYBRIDGE)
#define  USE_PSHUFD
#else
#define USE_PSHUFD_HALF
#endif

#include "l1param.h"

	PROLOGUE
	PROFCODE

	pushl	%edi
	pushl	%esi
	pushl	%ebx
	pushl	%ebp

	movl	STACK_M,     M
	movl	STACK_X,     X
	movl	STACK_INCX,  INCX

	movss	STACK_ALPHA_R, %xmm0
	movss	STACK_ALPHA_I, %xmm1

	sall	$ZBASE_SHIFT, INCX
	xor	FLAG, FLAG

	testl	M, M
	jle	.L999

	xorps	%xmm7, %xmm7
	comiss	%xmm0, %xmm7
	jne	.L100		# Alpha_r != ZERO

	comiss	%xmm1, %xmm7
	jne	.L100		# Alpha_i != ZERO

/* Alpha == ZERO */
	cmpl	$2 * SIZE, INCX
	jne	.L50

/* INCX == 1 */
	cmpl	$3, M
	jle	.L13

	testl	$4, X
	je	.L05
	movss	%xmm7, 0 * SIZE(X)
	addl	$SIZE, X
	movl	$1, FLAG
	decl	M
	ALIGN_3

.L05:
	testl	$8, X
	je	.L06

	movlps	%xmm7, 0 * SIZE(X)
	addl	$2 * SIZE, X
	subl	$1, M
	ALIGN_3
.L06:

	movl	M,  I	# rcx = n
	sarl	$3, I
	jle	.L12
	ALIGN_4

.L11:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movaps	%xmm7,  0 * SIZE(X)
	movaps	%xmm7,  4 * SIZE(X)
	movaps	%xmm7,  8 * SIZE(X)
	movaps	%xmm7, 12 * SIZE(X)
	addl	$16 * SIZE, X
	decl	I
	jg	.L11
	ALIGN_4

.L12:
	testl	$7, M
	je	.L19
	testl	$4, M
	je	.L13

	movaps	%xmm7,  0 * SIZE(X)
	movaps	%xmm7,  4 * SIZE(X)
	addl	$8 * SIZE, X
	ALIGN_3

.L13:
	testl  $2, M
	je    .L14

	movlps	%xmm7,  0 * SIZE(X)
	movhps	%xmm7,  2 * SIZE(X)
	addl	$4 * SIZE, X
	ALIGN_3

.L14:
	testl  $1, M
	je    .L19

	movlps	%xmm7,  0 * SIZE(X)
	addl	$2 * SIZE, X
	ALIGN_3

.L19:
	testl  $1, FLAG
	je    .L999

	movss	%xmm7, 0 * SIZE(X)
	jmp	.L999
	ALIGN_4

/* incx != 1 */
.L50:
	movl	M,  I		# rcx = n
	sarl	$2, I
	jle	.L52
	ALIGN_4

.L51:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movsd	%xmm7, 0 * SIZE(X)
	addl	INCX, X
	movsd	%xmm7, 0 * SIZE(X)
	addl	INCX, X
	movsd	%xmm7, 0 * SIZE(X)
	addl	INCX, X
	movsd	%xmm7, 0 * SIZE(X)
	addl	INCX, X
	decl	I
	jg	.L51
	ALIGN_4

.L52:
	testl	$2, M
	je	.L53

	movsd	%xmm7, 0 * SIZE(X)
	addl	INCX, X
	movsd	%xmm7, 0 * SIZE(X)
	addl	INCX, X
	ALIGN_3

.L53:
	testl	$1, M
	je	.L999

	movsd	%xmm7, 0 * SIZE(X)
	jmp	.L999
	ALIGN_4

/* Alpha != ZERO */

.L100:
	testl	$SIZE, X
	jne	.L130

	cmpl	$2 * SIZE, INCX
	jne	.L120

	movaps	%xmm0, %xmm6
	shufps	$0, %xmm6, %xmm6
	shufps	$0, %xmm1, %xmm1
	subps	  %xmm1,   %xmm7
	unpcklps  %xmm1,   %xmm7

	subl	$-32 * SIZE, X

	testl	$2 * SIZE, X
	je	.L105

	movsd	-32 * SIZE(X), %xmm0

	PSHUFD2( $0xb1, %xmm0, %xmm5)
	mulps	 %xmm6, %xmm0
	mulps	 %xmm7, %xmm5
	addps	 %xmm5, %xmm0

	movlps	%xmm0,  -32 * SIZE(X)
	addl	$2 * SIZE, X
	decl	M
	jle	.L999
	ALIGN_3

.L105:
	movl	M,  I
	sarl	$4, I
	jle	.L115

	movaps	-32 * SIZE(X), %xmm0
	movaps	-28 * SIZE(X), %xmm1
	movaps	-24 * SIZE(X), %xmm2
	movaps	-20 * SIZE(X), %xmm3

	decl	I
	jle	.L112
	ALIGN_4

.L111:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	PSHUFD2( $0xb1, %xmm0, %xmm5)
	mulps	 %xmm6, %xmm0
	mulps	 %xmm7, %xmm5
	addps	 %xmm5, %xmm0
	movaps	%xmm0,  -32 * SIZE(X)
	movaps	-16 * SIZE(X), %xmm0

	PSHUFD2( $0xb1, %xmm1, %xmm5)
	mulps	 %xmm6, %xmm1
	mulps	 %xmm7, %xmm5
	addps	 %xmm5, %xmm1
	movaps	%xmm1,  -28 * SIZE(X)
	movaps	-12 * SIZE(X), %xmm1

	PSHUFD2( $0xb1, %xmm2, %xmm5)
	mulps	 %xmm6, %xmm2
	mulps	 %xmm7, %xmm5
	addps	 %xmm5, %xmm2
	movaps	%xmm2,  -24 * SIZE(X)
	movaps	 -8 * SIZE(X), %xmm2

	PSHUFD2( $0xb1, %xmm3, %xmm5)
	mulps	 %xmm6, %xmm3
	mulps	 %xmm7, %xmm5
	addps	 %xmm5, %xmm3
	movaps	%xmm3,  -20 * SIZE(X)
	movaps	 -4 * SIZE(X), %xmm3

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE + 64) - PREOFFSET(X)
#endif

	PSHUFD2( $0xb1, %xmm0, %xmm5)
	mulps	 %xmm6, %xmm0
	mulps	 %xmm7, %xmm5
	addps	 %xmm5, %xmm0
	movaps	%xmm0,  -16 * SIZE(X)
	movaps	  0 * SIZE(X), %xmm0

	PSHUFD2( $0xb1, %xmm1, %xmm5)
	mulps	 %xmm6, %xmm1
	mulps	 %xmm7, %xmm5
	addps	 %xmm5, %xmm1
	movaps	%xmm1,  -12 * SIZE(X)
	movaps	  4 * SIZE(X), %xmm1

	PSHUFD2( $0xb1, %xmm2, %xmm5)
	mulps	 %xmm6, %xmm2
	mulps	 %xmm7, %xmm5
	addps	 %xmm5, %xmm2
	movaps	%xmm2,   -8 * SIZE(X)
	movaps	  8 * SIZE(X), %xmm2

	PSHUFD2( $0xb1, %xmm3, %xmm5)
	mulps	 %xmm6, %xmm3
	mulps	 %xmm7, %xmm5
	addps	 %xmm5, %xmm3
	movaps	%xmm3,   -4 * SIZE(X)
	movaps	 12 * SIZE(X), %xmm3

	subl	$-32 * SIZE, X
	decl	I
	jg	.L111
	ALIGN_4

.L112:
	PSHUFD2( $0xb1, %xmm0, %xmm5)
	mulps	 %xmm6, %xmm0
	mulps	 %xmm7, %xmm5
	addps	 %xmm5, %xmm0
	movaps	%xmm0,  -32 * SIZE(X)
	movaps	-16 * SIZE(X), %xmm0

	PSHUFD2( $0xb1, %xmm1, %xmm5)
	mulps	 %xmm6, %xmm1
	mulps	 %xmm7, %xmm5
	addps	 %xmm5, %xmm1
	movaps	%xmm1,  -28 * SIZE(X)
	movaps	-12 * SIZE(X), %xmm1

	PSHUFD2( $0xb1, %xmm2, %xmm5)
	mulps	 %xmm6, %xmm2
	mulps	 %xmm7, %xmm5
	addps	 %xmm5, %xmm2
	movaps	%xmm2,  -24 * SIZE(X)
	movaps	 -8 * SIZE(X), %xmm2

	PSHUFD2( $0xb1, %xmm3, %xmm5)
	mulps	 %xmm6, %xmm3
	mulps	 %xmm7, %xmm5
	addps	 %xmm5, %xmm3
	movaps	%xmm3,  -20 * SIZE(X)
	movaps	 -4 * SIZE(X), %xmm3

	PSHUFD2( $0xb1, %xmm0, %xmm5)
	mulps	 %xmm6, %xmm0
	mulps	 %xmm7, %xmm5
	addps	 %xmm5, %xmm0
	movaps	%xmm0,  -16 * SIZE(X)

	PSHUFD2( $0xb1, %xmm1, %xmm5)
	mulps	 %xmm6, %xmm1
	mulps	 %xmm7, %xmm5
	addps	 %xmm5, %xmm1
	movaps	%xmm1,  -12 * SIZE(X)

	PSHUFD2( $0xb1, %xmm2, %xmm5)
	mulps	 %xmm6, %xmm2
	mulps	 %xmm7, %xmm5
	addps	 %xmm5, %xmm2
	movaps	%xmm2,   -8 * SIZE(X)

	PSHUFD2( $0xb1, %xmm3, %xmm5)
	mulps	 %xmm6, %xmm3
	mulps	 %xmm7, %xmm5
	addps	 %xmm5, %xmm3
	movaps	%xmm3,   -4 * SIZE(X)

	subl	$-32 * SIZE, X
	ALIGN_4

.L115:
	testl	$8, M
	je	.L116

	movaps	-32 * SIZE(X), %xmm0
	movaps	-28 * SIZE(X), %xmm1

	PSHUFD2( $0xb1, %xmm0, %xmm5)
	mulps	 %xmm6, %xmm0
	mulps	 %xmm7, %xmm5
	addps	 %xmm5, %xmm0
	movaps	%xmm0,  -32 * SIZE(X)

	PSHUFD2( $0xb1, %xmm1, %xmm5)
	mulps	 %xmm6, %xmm1
	mulps	 %xmm7, %xmm5
	addps	 %xmm5, %xmm1
	movaps	%xmm1,  -28 * SIZE(X)

	movaps	-24 * SIZE(X), %xmm2
	movaps	-20 * SIZE(X), %xmm3

	PSHUFD2( $0xb1, %xmm2, %xmm5)
	mulps	 %xmm6, %xmm2
	mulps	 %xmm7, %xmm5
	addps	 %xmm5, %xmm2
	movaps	%xmm2,  -24 * SIZE(X)

	PSHUFD2( $0xb1, %xmm3, %xmm5)
	mulps	 %xmm6, %xmm3
	mulps	 %xmm7, %xmm5
	addps	 %xmm5, %xmm3
	movaps	%xmm3,  -20 * SIZE(X)

	addl	$16 * SIZE, X
	ALIGN_3

.L116:
	testl	$4, M
	je	.L117

	movaps	-32 * SIZE(X), %xmm0
	movaps	-28 * SIZE(X), %xmm1

	PSHUFD2( $0xb1, %xmm0, %xmm5)
	mulps	 %xmm6, %xmm0
	mulps	 %xmm7, %xmm5
	addps	 %xmm5, %xmm0
	movaps	%xmm0,  -32 * SIZE(X)

	PSHUFD2( $0xb1, %xmm1, %xmm5)
	mulps	 %xmm6, %xmm1
	mulps	 %xmm7, %xmm5
	addps	 %xmm5, %xmm1
	movaps	%xmm1,  -28 * SIZE(X)

	addl	$8 * SIZE, X
	ALIGN_3

.L117:
	testl	$2, M
	je	.L118

	movaps	-32 * SIZE(X), %xmm0

	PSHUFD2( $0xb1, %xmm0, %xmm5)
	mulps	 %xmm6, %xmm0
	mulps	 %xmm7, %xmm5
	addps	 %xmm5, %xmm0
	movaps	%xmm0,  -32 * SIZE(X)

	addl	$4 * SIZE, X
	ALIGN_3

.L118:
	testl	$1, M
	je	.L999

	movsd	-32 * SIZE(X), %xmm0

	PSHUFD2( $0xb1, %xmm0, %xmm5)
	mulps	 %xmm6, %xmm0
	mulps	 %xmm7, %xmm5
	addps	 %xmm5, %xmm0

	movlps	%xmm0,  -32 * SIZE(X)
	jmp	.L999
	ALIGN_3

.L120:
	PSHUFD2($0, %xmm0, %xmm6)
	PSHUFD2($0, %xmm1, %xmm1)
	subps	  %xmm1,   %xmm7
	unpcklps  %xmm1,   %xmm7

	movl	X, XX

	movl	M,  I
	sarl	$3, I
	jle	.L125

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

	decl	I
	jle	.L122
	ALIGN_4

.L121:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	PSHUFD2( $0xb1,  %xmm0, %xmm5)
	mulps	 %xmm6, %xmm0
	mulps	 %xmm7, %xmm5
	addps	 %xmm5,  %xmm0

	movlps	%xmm0,  (XX)
	addl	INCX, XX
	movhps	%xmm0,  (XX)
	addl	INCX, XX

	movsd	(X), %xmm0
	addl	INCX, X
	movhps	(X), %xmm0
	addl	INCX, X

	PSHUFD2( $0xb1,  %xmm1, %xmm5)
	mulps	 %xmm6, %xmm1
	mulps	 %xmm7, %xmm5
	addps	 %xmm5,  %xmm1

	movlps	%xmm1,  (XX)
	addl	INCX, XX
	movhps	%xmm1,  (XX)
	addl	INCX, XX

	movsd	(X), %xmm1
	addl	INCX, X
	movhps	(X), %xmm1
	addl	INCX, X

	PSHUFD2( $0xb1,  %xmm2, %xmm5)
	mulps	 %xmm6, %xmm2
	mulps	 %xmm7, %xmm5
	addps	 %xmm5,  %xmm2

	movlps	%xmm2,  (XX)
	addl	INCX, XX
	movhps	%xmm2,  (XX)
	addl	INCX, XX

	movsd	(X), %xmm2
	addl	INCX, X
	movhps	(X), %xmm2
	addl	INCX, X

	PSHUFD2( $0xb1,  %xmm3, %xmm5)
	mulps	 %xmm6, %xmm3
	mulps	 %xmm7, %xmm5
	addps	 %xmm5,  %xmm3

	movlps	%xmm3,  (XX)
	addl	INCX, XX
	movhps	%xmm3,  (XX)
	addl	INCX, XX

	movsd	(X), %xmm3
	addl	INCX, X
	movhps	(X), %xmm3
	addl	INCX, X

	decl	I
	jg	.L121
	ALIGN_4

.L122:
	PSHUFD2( $0xb1,  %xmm0, %xmm5)
	mulps	 %xmm6, %xmm0
	mulps	 %xmm7, %xmm5
	addps	 %xmm5,  %xmm0

	movlps	%xmm0,  (XX)
	addl	INCX, XX
	movhps	%xmm0,  (XX)
	addl	INCX, XX

	PSHUFD2( $0xb1,  %xmm1, %xmm5)
	mulps	 %xmm6, %xmm1
	mulps	 %xmm7, %xmm5
	addps	 %xmm5,  %xmm1

	movlps	%xmm1,  (XX)
	addl	INCX, XX
	movhps	%xmm1,  (XX)
	addl	INCX, XX

	PSHUFD2( $0xb1,  %xmm2, %xmm5)
	mulps	 %xmm6, %xmm2
	mulps	 %xmm7, %xmm5
	addps	 %xmm5,  %xmm2

	movlps	%xmm2,  (XX)
	addl	INCX, XX
	movhps	%xmm2,  (XX)
	addl	INCX, XX

	PSHUFD2( $0xb1,  %xmm3, %xmm5)
	mulps	 %xmm6, %xmm3
	mulps	 %xmm7, %xmm5
	addps	 %xmm5,  %xmm3

	movlps	%xmm3,  (XX)
	addl	INCX, XX
	movhps	%xmm3,  (XX)
	addl	INCX, XX
	ALIGN_4

.L125:
	testl	$4, M
	je	.L127

	movsd	(X), %xmm0
	addl	INCX, X
	movhps	(X), %xmm0
	addl	INCX, X

	PSHUFD2( $0xb1,  %xmm0, %xmm5)
	mulps	 %xmm6, %xmm0
	mulps	 %xmm7, %xmm5
	addps	 %xmm5,  %xmm0

	movlps	%xmm0,  (XX)
	addl	INCX, XX
	movhps	%xmm0,  (XX)
	addl	INCX, XX

	movsd	(X), %xmm1
	addl	INCX, X
	movhps	(X), %xmm1
	addl	INCX, X

	PSHUFD2( $0xb1,  %xmm1, %xmm5)
	mulps	 %xmm6, %xmm1
	mulps	 %xmm7, %xmm5
	addps	 %xmm5,  %xmm1

	movlps	%xmm1,  (XX)
	addl	INCX, XX
	movhps	%xmm1,  (XX)
	addl	INCX, XX
	ALIGN_3

.L127:
	testl	$2, M
	je	.L128

	movsd	(X), %xmm0
	addl	INCX, X
	movhps	(X), %xmm0
	addl	INCX, X

	PSHUFD2( $0xb1,  %xmm0, %xmm5)
	mulps	 %xmm6, %xmm0
	mulps	 %xmm7, %xmm5
	addps	 %xmm5,  %xmm0

	movlps	%xmm0,  (XX)
	addl	INCX, XX
	movhps	%xmm0,  (XX)
	addl	INCX, XX
	ALIGN_3

.L128:
	testl	$1, M
	je	.L999

	movsd	(X), %xmm0

	PSHUFD2( $0xb1,  %xmm0, %xmm5)
	mulps	 %xmm6, %xmm0
	mulps	 %xmm7, %xmm5
	addps	 %xmm5,  %xmm0

	movlps	%xmm0,  (XX)
	jmp	.L999
	ALIGN_3

.L130:
	cmpl	$2 * SIZE, INCX
	jne	.L120

#if defined(ALIGNED_ACCESS) && !defined(NEHALEM) &&  !defined(SANDYBRIDGE)

	PSHUFD2($0, %xmm0, %xmm6)
	PSHUFD2($0, %xmm1, %xmm1)
	subps	  %xmm1,   %xmm7
	unpcklps  %xmm1,   %xmm7

	subl	$-31 * SIZE, X

	testl	$2 * SIZE, X
	je	.L130x

	movsd	-31 * SIZE(X), %xmm0

	PSHUFD2( $0xb1, %xmm0, %xmm5)
	mulps	 %xmm6, %xmm0
	mulps	 %xmm7, %xmm5
	addps	 %xmm5, %xmm0

	movlps	%xmm0,  -31 * SIZE(X)
	addl	$2 * SIZE, X
	decl	M
	jle	.L999
	ALIGN_3

.L130x:
	shufps	$0xb1, %xmm7, %xmm7

	movaps	-32 * SIZE(X), %xmm0
	movaps	%xmm0, %xmm4

	movl	M,  I
	sarl	$4, I
	jle	.L135

	movaps	-28 * SIZE(X), %xmm1


	decl	I
	jle	.L132
	ALIGN_4

.L131:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movss	%xmm1, %xmm0
	PSHUFD2($0x1b, %xmm0, %xmm5)
	mulps	%xmm6, %xmm0
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm0

	movaps	%xmm0, %xmm2
	movss	%xmm4, %xmm0
	movaps	%xmm0,  -32 * SIZE(X)

	movaps	-24 * SIZE(X), %xmm0

	movss	%xmm0, %xmm1
	PSHUFD2($0x1b, %xmm1, %xmm5)
	mulps	%xmm6, %xmm1
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm1

	movaps	%xmm1, %xmm4
	movss	%xmm2, %xmm1
	movaps	%xmm1,  -28 * SIZE(X)

	movaps	-20 * SIZE(X), %xmm1

	movss	%xmm1, %xmm0
	PSHUFD2($0x1b, %xmm0, %xmm5)
	mulps	%xmm6, %xmm0
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm0

	movaps	%xmm0, %xmm2
	movss	%xmm4, %xmm0
	movaps	%xmm0,  -24 * SIZE(X)

	movaps	-16 * SIZE(X), %xmm0

	movss	%xmm0, %xmm1
	PSHUFD2($0x1b, %xmm1, %xmm5)
	mulps	%xmm6, %xmm1
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm1

	movaps	%xmm1, %xmm4
	movss	%xmm2, %xmm1
	movaps	%xmm1,  -20 * SIZE(X)

	movaps	-12 * SIZE(X), %xmm1

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE + 64) - PREOFFSET(X)
#endif

	movss	%xmm1, %xmm0
	PSHUFD2($0x1b, %xmm0, %xmm5)
	mulps	%xmm6, %xmm0
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm0

	movaps	%xmm0, %xmm2
	movss	%xmm4, %xmm0
	movaps	%xmm0,  -16 * SIZE(X)

	movaps	 -8 * SIZE(X), %xmm0

	movss	%xmm0, %xmm1
	PSHUFD2($0x1b, %xmm1, %xmm5)
	mulps	%xmm6, %xmm1
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm1

	movaps	%xmm1, %xmm4
	movss	%xmm2, %xmm1
	movaps	%xmm1,  -12 * SIZE(X)

	movaps	 -4 * SIZE(X), %xmm1

	movss	%xmm1, %xmm0
	PSHUFD2($0x1b, %xmm0, %xmm5)
	mulps	%xmm6, %xmm0
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm0

	movaps	%xmm0, %xmm2
	movss	%xmm4, %xmm0
	movaps	%xmm0,   -8 * SIZE(X)

	movaps	  0 * SIZE(X), %xmm0

	movss	%xmm0, %xmm1
	PSHUFD2($0x1b, %xmm1, %xmm5)
	mulps	%xmm6, %xmm1
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm1

	movaps	%xmm1, %xmm4
	movss	%xmm2, %xmm1
	movaps	%xmm1,   -4 * SIZE(X)

	movaps	  4 * SIZE(X), %xmm1

	subl	$-32 * SIZE, X
	decl	I
	jg	.L131
	ALIGN_4

.L132:
	movss	%xmm1, %xmm0
	PSHUFD2($0x1b, %xmm0, %xmm5)
	mulps	%xmm6, %xmm0
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm0

	movaps	%xmm0, %xmm2
	movss	%xmm4, %xmm0
	movaps	%xmm0,  -32 * SIZE(X)

	movaps	-24 * SIZE(X), %xmm0

	movss	%xmm0, %xmm1
	PSHUFD2($0x1b, %xmm1, %xmm5)
	mulps	%xmm6, %xmm1
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm1

	movaps	%xmm1, %xmm4
	movss	%xmm2, %xmm1
	movaps	%xmm1,  -28 * SIZE(X)

	movaps	-20 * SIZE(X), %xmm1

	movss	%xmm1, %xmm0
	PSHUFD2($0x1b, %xmm0, %xmm5)
	mulps	%xmm6, %xmm0
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm0

	movaps	%xmm0, %xmm2
	movss	%xmm4, %xmm0
	movaps	%xmm0,  -24 * SIZE(X)

	movaps	-16 * SIZE(X), %xmm0

	movss	%xmm0, %xmm1
	PSHUFD2($0x1b, %xmm1, %xmm5)
	mulps	%xmm6, %xmm1
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm1

	movaps	%xmm1, %xmm4
	movss	%xmm2, %xmm1
	movaps	%xmm1,  -20 * SIZE(X)

	movaps	-12 * SIZE(X), %xmm1

	movss	%xmm1, %xmm0
	PSHUFD2($0x1b, %xmm0, %xmm5)
	mulps	%xmm6, %xmm0
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm0

	movaps	%xmm0, %xmm2
	movss	%xmm4, %xmm0
	movaps	%xmm0,  -16 * SIZE(X)

	movaps	 -8 * SIZE(X), %xmm0

	movss	%xmm0, %xmm1
	PSHUFD2($0x1b, %xmm1, %xmm5)
	mulps	%xmm6, %xmm1
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm1

	movaps	%xmm1, %xmm4
	movss	%xmm2, %xmm1
	movaps	%xmm1,  -12 * SIZE(X)

	movaps	 -4 * SIZE(X), %xmm1

	movss	%xmm1, %xmm0
	PSHUFD2($0x1b, %xmm0, %xmm5)
	mulps	%xmm6, %xmm0
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm0

	movaps	%xmm0, %xmm2
	movss	%xmm4, %xmm0
	movaps	%xmm0,   -8 * SIZE(X)

	movaps	  0 * SIZE(X), %xmm0

	movss	%xmm0, %xmm1
	PSHUFD2($0x1b, %xmm1, %xmm5)
	mulps	%xmm6, %xmm1
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm1

	movaps	%xmm1, %xmm4
	movss	%xmm2, %xmm1
	movaps	%xmm1,   -4 * SIZE(X)

	subl	$-32 * SIZE, X
	ALIGN_4

.L135:
	testl	$8, M
	je	.L136

	movaps	-28 * SIZE(X), %xmm1

	movss	%xmm1, %xmm0
	PSHUFD2($0x1b, %xmm0, %xmm5)
	mulps	%xmm6, %xmm0
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm0

	movaps	%xmm0, %xmm2
	movss	%xmm4, %xmm0
	movaps	%xmm0,  -32 * SIZE(X)

	movaps	-24 * SIZE(X), %xmm0

	movss	%xmm0, %xmm1
	PSHUFD2($0x1b, %xmm1, %xmm5)
	mulps	%xmm6, %xmm1
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm1

	movaps	%xmm1, %xmm4
	movss	%xmm2, %xmm1
	movaps	%xmm1,  -28 * SIZE(X)

	movaps	-20 * SIZE(X), %xmm1

	movss	%xmm1, %xmm0
	PSHUFD2($0x1b, %xmm0, %xmm5)
	mulps	%xmm6, %xmm0
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm0

	movaps	%xmm0, %xmm2
	movss	%xmm4, %xmm0
	movaps	%xmm0,  -24 * SIZE(X)

	movaps	-16 * SIZE(X), %xmm0

	movss	%xmm0, %xmm1
	PSHUFD2($0x1b, %xmm1, %xmm5)
	mulps	%xmm6, %xmm1
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm1

	movaps	%xmm1, %xmm4
	movss	%xmm2, %xmm1
	movaps	%xmm1,  -20 * SIZE(X)

	addl	$16 * SIZE, X
	ALIGN_3

.L136:
	testl	$4, M
	je	.L137

	movaps	-28 * SIZE(X), %xmm1

	movss	%xmm1, %xmm0
	PSHUFD2($0x1b, %xmm0, %xmm5)
	mulps	%xmm6, %xmm0
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm0

	movaps	%xmm0, %xmm2
	movss	%xmm4, %xmm0
	movaps	%xmm0,  -32 * SIZE(X)

	movaps	-24 * SIZE(X), %xmm0

	movss	%xmm0, %xmm1
	PSHUFD2($0x1b, %xmm1, %xmm5)
	mulps	%xmm6, %xmm1
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm1

	movaps	%xmm1, %xmm4
	movss	%xmm2, %xmm1
	movaps	%xmm1,  -28 * SIZE(X)

	addl	$8 * SIZE, X
	ALIGN_3

.L137:
	testl	$2, M
	je	.L138

	movaps	-28 * SIZE(X), %xmm1

	movss	%xmm1, %xmm0
	PSHUFD2($0x1b, %xmm0, %xmm5)
	mulps	%xmm6, %xmm0
	mulps	%xmm7, %xmm5
	addps	%xmm5, %xmm0

	movaps	%xmm0, %xmm2
	movss	%xmm4, %xmm0
	movaps	%xmm0,  -32 * SIZE(X)
	movaps	%xmm2, %xmm4
	movaps	%xmm1, %xmm0

	addl	$4 * SIZE, X
	ALIGN_3

.L138:
	movss	%xmm4, -32 * SIZE(X)

	testl	$1, M
	je	.L999

	PSHUFD2( $0x1b,  %xmm0, %xmm5)
	mulps	 %xmm6, %xmm0
	mulps	 %xmm7, %xmm5
	addps	 %xmm5,  %xmm0

	PSHUFD1( $0x39, %xmm0)

	movlps	%xmm0,  -31 * SIZE(X)
	jmp	.L999
	ALIGN_3


#else

	PSHUFD2($0, %xmm0, %xmm6)
	PSHUFD2($0, %xmm1, %xmm1)
	subps	  %xmm1,   %xmm7
	unpcklps  %xmm1,   %xmm7

	subl	$-32 * SIZE, X

	testl	$2 * SIZE, X
	je	.L130x

#ifdef movsd
	xorps	%xmm0, %xmm0
#endif
	movsd	-32 * SIZE(X), %xmm0

	PSHUFD2( $0xb1,  %xmm0, %xmm5)
	mulps	 %xmm6, %xmm0
	mulps	 %xmm7, %xmm5
	addps	 %xmm5,  %xmm0

	movlps	%xmm0,  -32 * SIZE(X)
	addl	$2 * SIZE, X
	decl	M
	jle	.L999
	ALIGN_3

.L130x:
	movl	M,  I
	sarl	$4, I
	jle	.L135

	movsd	-32 * SIZE(X), %xmm0
	movhps	-30 * SIZE(X), %xmm0
	movsd	-28 * SIZE(X), %xmm1
	movhps	-26 * SIZE(X), %xmm1
	movsd	-24 * SIZE(X), %xmm2
	movhps	-22 * SIZE(X), %xmm2
	movsd	-20 * SIZE(X), %xmm3
	movhps	-18 * SIZE(X), %xmm3

	decl	I
	jle	.L132
	ALIGN_4

.L131:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	PSHUFD2( $0xb1, %xmm0, %xmm5)
	mulps	 %xmm6, %xmm0
	mulps	 %xmm7, %xmm5
	addps	 %xmm5, %xmm0
	movlps	%xmm0,  -32 * SIZE(X)
	movhps	%xmm0,  -30 * SIZE(X)
	movsd	-16 * SIZE(X), %xmm0
	movhps	-14 * SIZE(X), %xmm0

	PSHUFD2( $0xb1, %xmm1, %xmm5)
	mulps	 %xmm6, %xmm1
	mulps	 %xmm7, %xmm5
	addps	 %xmm5, %xmm1
	movlps	%xmm1,  -28 * SIZE(X)
	movhps	%xmm1,  -26 * SIZE(X)
	movsd	-12 * SIZE(X), %xmm1
	movhps	-10 * SIZE(X), %xmm1

	PSHUFD2( $0xb1, %xmm2, %xmm5)
	mulps	 %xmm6, %xmm2
	mulps	 %xmm7, %xmm5
	addps	 %xmm5, %xmm2
	movlps	%xmm2,  -24 * SIZE(X)
	movhps	%xmm2,  -22 * SIZE(X)
	movsd	 -8 * SIZE(X), %xmm2
	movhps	 -6 * SIZE(X), %xmm2

	PSHUFD2( $0xb1, %xmm3, %xmm5)
	mulps	 %xmm6, %xmm3
	mulps	 %xmm7, %xmm5
	addps	 %xmm5, %xmm3
	movlps	%xmm3,  -20 * SIZE(X)
	movhps	%xmm3,  -18 * SIZE(X)
	movsd	 -4 * SIZE(X), %xmm3
	movhps	 -2 * SIZE(X), %xmm3

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE + 64) - PREOFFSET(X)
#endif

	PSHUFD2( $0xb1, %xmm0, %xmm5)
	mulps	 %xmm6, %xmm0
	mulps	 %xmm7, %xmm5
	addps	 %xmm5, %xmm0
	movlps	%xmm0,  -16 * SIZE(X)
	movhps	%xmm0,  -14 * SIZE(X)
	movsd	  0 * SIZE(X), %xmm0
	movhps	  2 * SIZE(X), %xmm0

	PSHUFD2( $0xb1, %xmm1, %xmm5)
	mulps	 %xmm6, %xmm1
	mulps	 %xmm7, %xmm5
	addps	 %xmm5, %xmm1
	movlps	%xmm1,  -12 * SIZE(X)
	movhps	%xmm1,  -10 * SIZE(X)
	movsd	  4 * SIZE(X), %xmm1
	movhps	  6 * SIZE(X), %xmm1

	PSHUFD2( $0xb1, %xmm2, %xmm5)
	mulps	 %xmm6, %xmm2
	mulps	 %xmm7, %xmm5
	addps	 %xmm5, %xmm2
	movlps	%xmm2,   -8 * SIZE(X)
	movhps	%xmm2,   -6 * SIZE(X)
	movsd	  8 * SIZE(X), %xmm2
	movhps	 10 * SIZE(X), %xmm2

	PSHUFD2( $0xb1, %xmm3, %xmm5)
	mulps	 %xmm6, %xmm3
	mulps	 %xmm7, %xmm5
	addps	 %xmm5, %xmm3
	movlps	%xmm3,   -4 * SIZE(X)
	movhps	%xmm3,   -2 * SIZE(X)
	movsd	 12 * SIZE(X), %xmm3
	movhps	 14 * SIZE(X), %xmm3

	subl	$-32 * SIZE, X
	decl	I
	jg	.L131
	ALIGN_4

.L132:
	PSHUFD2( $0xb1, %xmm0, %xmm5)
	mulps	 %xmm6, %xmm0
	mulps	 %xmm7, %xmm5
	addps	 %xmm5, %xmm0
	movlps	%xmm0,  -32 * SIZE(X)
	movhps	%xmm0,  -30 * SIZE(X)
	movsd	-16 * SIZE(X), %xmm0
	movhps	-14 * SIZE(X), %xmm0

	PSHUFD2( $0xb1, %xmm1, %xmm5)
	mulps	 %xmm6, %xmm1
	mulps	 %xmm7, %xmm5
	addps	 %xmm5, %xmm1
	movlps	%xmm1,  -28 * SIZE(X)
	movhps	%xmm1,  -26 * SIZE(X)
	movsd	-12 * SIZE(X), %xmm1
	movhps	-10 * SIZE(X), %xmm1

	PSHUFD2( $0xb1, %xmm2, %xmm5)
	mulps	 %xmm6, %xmm2
	mulps	 %xmm7, %xmm5
	addps	 %xmm5, %xmm2
	movlps	%xmm2,  -24 * SIZE(X)
	movhps	%xmm2,  -22 * SIZE(X)
	movsd	 -8 * SIZE(X), %xmm2
	movhps	 -6 * SIZE(X), %xmm2

	PSHUFD2( $0xb1, %xmm3, %xmm5)
	mulps	 %xmm6, %xmm3
	mulps	 %xmm7, %xmm5
	addps	 %xmm5, %xmm3
	movlps	%xmm3,  -20 * SIZE(X)
	movhps	%xmm3,  -18 * SIZE(X)
	movsd	 -4 * SIZE(X), %xmm3
	movhps	 -2 * SIZE(X), %xmm3

	PSHUFD2( $0xb1, %xmm0, %xmm5)
	mulps	 %xmm6, %xmm0
	mulps	 %xmm7, %xmm5
	addps	 %xmm5, %xmm0
	movlps	%xmm0,  -16 * SIZE(X)
	movhps	%xmm0,  -14 * SIZE(X)

	PSHUFD2( $0xb1, %xmm1, %xmm5)
	mulps	 %xmm6, %xmm1
	mulps	 %xmm7, %xmm5
	addps	 %xmm5, %xmm1
	movlps	%xmm1,  -12 * SIZE(X)
	movhps	%xmm1,  -10 * SIZE(X)

	PSHUFD2( $0xb1, %xmm2, %xmm5)
	mulps	 %xmm6, %xmm2
	mulps	 %xmm7, %xmm5
	addps	 %xmm5, %xmm2
	movlps	%xmm2,   -8 * SIZE(X)
	movhps	%xmm2,   -6 * SIZE(X)

	PSHUFD2( $0xb1, %xmm3, %xmm5)
	mulps	 %xmm6, %xmm3
	mulps	 %xmm7, %xmm5
	addps	 %xmm5, %xmm3
	movlps	%xmm3,   -4 * SIZE(X)
	movhps	%xmm3,   -2 * SIZE(X)

	subl	$-32 * SIZE, X
	ALIGN_4

.L135:
	testl	$8, M
	je	.L136

	movsd	-32 * SIZE(X), %xmm0
	movhps	-30 * SIZE(X), %xmm0

	PSHUFD2( $0xb1, %xmm0, %xmm5)
	mulps	 %xmm6, %xmm0
	mulps	 %xmm7, %xmm5
	addps	 %xmm5, %xmm0
	movlps	%xmm0,  -32 * SIZE(X)
	movhps	%xmm0,  -30 * SIZE(X)

	movsd	-28 * SIZE(X), %xmm1
	movhps	-26 * SIZE(X), %xmm1

	PSHUFD2( $0xb1, %xmm1, %xmm5)
	mulps	 %xmm6, %xmm1
	mulps	 %xmm7, %xmm5
	addps	 %xmm5, %xmm1
	movlps	%xmm1,  -28 * SIZE(X)
	movhps	%xmm1,  -26 * SIZE(X)

	movsd	-24 * SIZE(X), %xmm2
	movhps	-22 * SIZE(X), %xmm2

	PSHUFD2( $0xb1, %xmm2, %xmm5)
	mulps	 %xmm6, %xmm2
	mulps	 %xmm7, %xmm5
	addps	 %xmm5, %xmm2
	movlps	%xmm2,  -24 * SIZE(X)
	movhps	%xmm2,  -22 * SIZE(X)

	movsd	-20 * SIZE(X), %xmm3
	movhps	-18 * SIZE(X), %xmm3

	PSHUFD2( $0xb1, %xmm3, %xmm5)
	mulps	 %xmm6, %xmm3
	mulps	 %xmm7, %xmm5
	addps	 %xmm5, %xmm3
	movlps	%xmm3,  -20 * SIZE(X)
	movhps	%xmm3,  -18 * SIZE(X)

	addl	$16 * SIZE, X
	ALIGN_3

.L136:
	testl	$4, M
	je	.L137

	movsd	-32 * SIZE(X), %xmm0
	movhps	-30 * SIZE(X), %xmm0
	movsd	-28 * SIZE(X), %xmm1
	movhps	-26 * SIZE(X), %xmm1

	PSHUFD2( $0xb1, %xmm0, %xmm5)
	mulps	 %xmm6, %xmm0
	mulps	 %xmm7, %xmm5
	addps	 %xmm5, %xmm0
	movlps	%xmm0,  -32 * SIZE(X)
	movhps	%xmm0,  -30 * SIZE(X)

	PSHUFD2( $0xb1, %xmm1, %xmm5)
	mulps	 %xmm6, %xmm1
	mulps	 %xmm7, %xmm5
	addps	 %xmm5, %xmm1
	movlps	%xmm1,  -28 * SIZE(X)
	movhps	%xmm1,  -26 * SIZE(X)

	addl	$8 * SIZE, X
	ALIGN_3

.L137:
	testl	$2, M
	je	.L138

	movsd	-32 * SIZE(X), %xmm0
	movhps	-30 * SIZE(X), %xmm0

	PSHUFD2( $0xb1, %xmm0, %xmm5)
	mulps	 %xmm6, %xmm0
	mulps	 %xmm7, %xmm5
	addps	 %xmm5, %xmm0
	movlps	%xmm0,  -32 * SIZE(X)
	movhps	%xmm0,  -30 * SIZE(X)

	addl	$4 * SIZE, X
	ALIGN_3

.L138:
	testl	$1, M
	je	.L999

	movsd	-32 * SIZE(X), %xmm0

	PSHUFD2( $0xb1, %xmm0, %xmm5)
	mulps	 %xmm6, %xmm0
	mulps	 %xmm7, %xmm5
	addps	 %xmm5, %xmm0

	movlps	%xmm0,  -32 * SIZE(X)
	ALIGN_3
#endif

.L999:
	xorl	%eax, %eax
	popl	%ebp
	popl	%ebx
	popl	%esi
	popl	%edi

	ret

	EPILOGUE
