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

#ifndef WINDOWS_ABI
#define M	ARG1
#define X	ARG4
#define INCX	ARG5
#else
#define M	ARG1
#define X	ARG2
#define INCX	ARG3
#endif

#define XX	%r10
#define FLAG	%r11
#define I	%rax

#include "l1param.h"

	PROLOGUE
	PROFCODE

#ifdef WINDOWS_ABI
	movaps	%xmm3, %xmm0
	movsd	40(%rsp), %xmm1
	movq	48(%rsp), X
	movq	56(%rsp), INCX
#endif

	SAVEREGISTERS

	salq	$ZBASE_SHIFT, INCX
	xor	FLAG, FLAG

	testq	M, M
	jle	.L999

	pxor	%xmm15, %xmm15
	comiss	%xmm0, %xmm15
	jne	.L100		# Alpha_r != ZERO

	comiss	%xmm1, %xmm15
	jne	.L100		# Alpha_i != ZERO

/* Alpha == ZERO */
	cmpq	$2 * SIZE, INCX
	jne	.L50

/* INCX == 1 */
	cmpq	$3, M
	jle	.L13

	testq	$4, X
	je	.L05
	movss	%xmm15, 0 * SIZE(X)
	addq	$SIZE, X
	movq	$1, FLAG
	decq	M
	ALIGN_3

.L05:
	testq	$8, X
	je	.L06

	movlps	%xmm15, 0 * SIZE(X)
	addq	$2 * SIZE, X
	subq	$1, M
	ALIGN_3
.L06:

	movq	M,  I	# rcx = n
	sarq	$3, I
	jle	.L12
	ALIGN_4

.L11:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movaps	%xmm15,  0 * SIZE(X)
	movaps	%xmm15,  4 * SIZE(X)
	movaps	%xmm15,  8 * SIZE(X)
	movaps	%xmm15, 12 * SIZE(X)
	addq	$16 * SIZE, X
	decq	I
	jg	.L11
	ALIGN_4

.L12:
	testq	$7, M
	je	.L19
	testq	$4, M
	je	.L13

	movaps	%xmm15,  0 * SIZE(X)
	movaps	%xmm15,  4 * SIZE(X)
	addq	$8 * SIZE, X
	ALIGN_3

.L13:
	testq  $2, M
	je    .L14

	movlps	%xmm15,  0 * SIZE(X)
	movhps	%xmm15,  2 * SIZE(X)
	addq	$4 * SIZE, X
	ALIGN_3

.L14:
	testq  $1, M
	je    .L19

	movlps	%xmm15,  0 * SIZE(X)
	addq	$2 * SIZE, X
	ALIGN_3

.L19:
	testq  $1, FLAG
	je    .L999

	movss	%xmm15, 0 * SIZE(X)
	jmp	.L999
	ALIGN_4

/* incx != 1 */
.L50:
	movq	M,  I		# rcx = n
	sarq	$2, I
	jle	.L52
	ALIGN_4

.L51:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movsd	%xmm15, 0 * SIZE(X)
	addq	INCX, X
	movsd	%xmm15, 0 * SIZE(X)
	addq	INCX, X
	movsd	%xmm15, 0 * SIZE(X)
	addq	INCX, X
	movsd	%xmm15, 0 * SIZE(X)
	addq	INCX, X
	decq	I
	jg	.L51
	ALIGN_4

.L52:
	testq	$2, M
	je	.L53

	movsd	%xmm15, 0 * SIZE(X)
	addq	INCX, X
	movsd	%xmm15, 0 * SIZE(X)
	addq	INCX, X
	ALIGN_3

.L53:
	testq	$1, M
	je	.L999

	movsd	%xmm15, 0 * SIZE(X)
	jmp	.L999
	ALIGN_4

/* Alpha != ZERO */

.L100:
	testq	$SIZE, X
	jne	.L130

	cmpq	$2 * SIZE, INCX
	jne	.L120

	pshufd	$0, %xmm0, %xmm14
	pshufd	$0, %xmm1, %xmm1
	subps	  %xmm1,   %xmm15
	unpcklps  %xmm1,   %xmm15

	subq	$-32 * SIZE, X

	testq	$2 * SIZE, X
	je	.L105

	movsd	-32 * SIZE(X), %xmm0

	pshufd	 $0xb1,  %xmm0, %xmm8
	mulps	 %xmm14, %xmm0
	mulps	 %xmm15, %xmm8
	addps	 %xmm8,  %xmm0

	movlps	%xmm0,  -32 * SIZE(X)
	addq	$2 * SIZE, X
	decq	M
	jle	.L999
	ALIGN_3

.L105:
	movq	M,  I
	sarq	$4, I
	jle	.L115

	movaps	-32 * SIZE(X), %xmm0
	movaps	-28 * SIZE(X), %xmm1
	movaps	-24 * SIZE(X), %xmm2
	movaps	-20 * SIZE(X), %xmm3
	movaps	-16 * SIZE(X), %xmm4
	movaps	-12 * SIZE(X), %xmm5
	movaps	 -8 * SIZE(X), %xmm6
	movaps	 -4 * SIZE(X), %xmm7

	decq	I
	jle	.L112
	ALIGN_4

.L111:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	pshufd	 $0xb1,  %xmm0, %xmm8
	mulps	 %xmm14, %xmm0
	mulps	 %xmm15, %xmm8
	addps	 %xmm8,  %xmm0
	movaps	%xmm0,  -32 * SIZE(X)
	movaps	  0 * SIZE(X), %xmm0

	pshufd	 $0xb1,  %xmm1, %xmm8
	mulps	 %xmm14, %xmm1
	mulps	 %xmm15, %xmm8
	addps	 %xmm8,  %xmm1
	movaps	%xmm1,  -28 * SIZE(X)
	movaps	  4 * SIZE(X), %xmm1

	pshufd	 $0xb1,  %xmm2, %xmm8
	mulps	 %xmm14, %xmm2
	mulps	 %xmm15, %xmm8
	addps	 %xmm8,  %xmm2
	movaps	%xmm2,  -24 * SIZE(X)
	movaps	  8 * SIZE(X), %xmm2

	pshufd	 $0xb1,  %xmm3, %xmm8
	mulps	 %xmm14, %xmm3
	mulps	 %xmm15, %xmm8
	addps	 %xmm8,  %xmm3
	movaps	%xmm3,  -20 * SIZE(X)
	movaps	 12 * SIZE(X), %xmm3

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE + 64) - PREOFFSET(X)
#endif

	pshufd	 $0xb1,  %xmm4, %xmm8
	mulps	 %xmm14, %xmm4
	mulps	 %xmm15, %xmm8
	addps	 %xmm8,  %xmm4
	movaps	%xmm4,  -16 * SIZE(X)
	movaps	 16 * SIZE(X), %xmm4

	pshufd	 $0xb1,  %xmm5, %xmm8
	mulps	 %xmm14, %xmm5
	mulps	 %xmm15, %xmm8
	addps	 %xmm8,  %xmm5
	movaps	%xmm5,  -12 * SIZE(X)
	movaps	 20 * SIZE(X), %xmm5

	pshufd	 $0xb1,  %xmm6, %xmm8
	mulps	 %xmm14, %xmm6
	mulps	 %xmm15, %xmm8
	addps	 %xmm8,  %xmm6
	movaps	%xmm6,   -8 * SIZE(X)
	movaps	 24 * SIZE(X), %xmm6

	pshufd	 $0xb1,  %xmm7, %xmm8
	mulps	 %xmm14, %xmm7
	mulps	 %xmm15, %xmm8
	addps	 %xmm8,  %xmm7
	movaps	%xmm7,   -4 * SIZE(X)
	movaps	 28 * SIZE(X), %xmm7

	subq	$-32 * SIZE, X
	decq	I
	jg	.L111
	ALIGN_4

.L112:
	pshufd	 $0xb1,  %xmm0, %xmm8
	mulps	 %xmm14, %xmm0
	mulps	 %xmm15, %xmm8
	addps	 %xmm8,  %xmm0
	movaps	%xmm0,  -32 * SIZE(X)

	pshufd	 $0xb1,  %xmm1, %xmm8
	mulps	 %xmm14, %xmm1
	mulps	 %xmm15, %xmm8
	addps	 %xmm8,  %xmm1
	movaps	%xmm1,  -28 * SIZE(X)

	pshufd	 $0xb1,  %xmm2, %xmm8
	mulps	 %xmm14, %xmm2
	mulps	 %xmm15, %xmm8
	addps	 %xmm8,  %xmm2
	movaps	%xmm2,  -24 * SIZE(X)

	pshufd	 $0xb1,  %xmm3, %xmm8
	mulps	 %xmm14, %xmm3
	mulps	 %xmm15, %xmm8
	addps	 %xmm8,  %xmm3
	movaps	%xmm3,  -20 * SIZE(X)

	pshufd	 $0xb1,  %xmm4, %xmm8
	mulps	 %xmm14, %xmm4
	mulps	 %xmm15, %xmm8
	addps	 %xmm8,  %xmm4
	movaps	%xmm4,  -16 * SIZE(X)

	pshufd	 $0xb1,  %xmm5, %xmm8
	mulps	 %xmm14, %xmm5
	mulps	 %xmm15, %xmm8
	addps	 %xmm8,  %xmm5
	movaps	%xmm5,  -12 * SIZE(X)

	pshufd	 $0xb1,  %xmm6, %xmm8
	mulps	 %xmm14, %xmm6
	mulps	 %xmm15, %xmm8
	addps	 %xmm8,  %xmm6
	movaps	%xmm6,   -8 * SIZE(X)

	pshufd	 $0xb1,  %xmm7, %xmm8
	mulps	 %xmm14, %xmm7
	mulps	 %xmm15, %xmm8
	addps	 %xmm8,  %xmm7
	movaps	%xmm7,   -4 * SIZE(X)

	subq	$-32 * SIZE, X
	ALIGN_4

.L115:
	testq	$8, M
	je	.L116

	movaps	-32 * SIZE(X), %xmm0
	movaps	-28 * SIZE(X), %xmm1

	pshufd	 $0xb1,  %xmm0, %xmm8
	mulps	 %xmm14, %xmm0
	mulps	 %xmm15, %xmm8
	addps	 %xmm8,  %xmm0
	movaps	%xmm0,  -32 * SIZE(X)

	pshufd	 $0xb1,  %xmm1, %xmm8
	mulps	 %xmm14, %xmm1
	mulps	 %xmm15, %xmm8
	addps	 %xmm8,  %xmm1
	movaps	%xmm1,  -28 * SIZE(X)

	movaps	-24 * SIZE(X), %xmm2
	movaps	-20 * SIZE(X), %xmm3

	pshufd	 $0xb1,  %xmm2, %xmm8
	mulps	 %xmm14, %xmm2
	mulps	 %xmm15, %xmm8
	addps	 %xmm8,  %xmm2
	movaps	%xmm2,  -24 * SIZE(X)

	pshufd	 $0xb1,  %xmm3, %xmm8
	mulps	 %xmm14, %xmm3
	mulps	 %xmm15, %xmm8
	addps	 %xmm8,  %xmm3
	movaps	%xmm3,  -20 * SIZE(X)

	addq	$16 * SIZE, X
	ALIGN_3

.L116:
	testq	$4, M
	je	.L117

	movaps	-32 * SIZE(X), %xmm0
	movaps	-28 * SIZE(X), %xmm1

	pshufd	 $0xb1,  %xmm0, %xmm8
	mulps	 %xmm14, %xmm0
	mulps	 %xmm15, %xmm8
	addps	 %xmm8,  %xmm0
	movaps	%xmm0,  -32 * SIZE(X)

	pshufd	 $0xb1,  %xmm1, %xmm8
	mulps	 %xmm14, %xmm1
	mulps	 %xmm15, %xmm8
	addps	 %xmm8,  %xmm1
	movaps	%xmm1,  -28 * SIZE(X)

	addq	$8 * SIZE, X
	ALIGN_3

.L117:
	testq	$2, M
	je	.L118

	movaps	-32 * SIZE(X), %xmm0

	pshufd	 $0xb1,  %xmm0, %xmm8
	mulps	 %xmm14, %xmm0
	mulps	 %xmm15, %xmm8
	addps	 %xmm8,  %xmm0
	movaps	%xmm0,  -32 * SIZE(X)

	addq	$4 * SIZE, X
	ALIGN_3

.L118:
	testq	$1, M
	je	.L999

	movsd	-32 * SIZE(X), %xmm0

	pshufd	 $0xb1,  %xmm0, %xmm8
	mulps	 %xmm14, %xmm0
	mulps	 %xmm15, %xmm8
	addps	 %xmm8,  %xmm0

	movlps	%xmm0,  -32 * SIZE(X)
	jmp	.L999
	ALIGN_3

.L120:
	pshufd	$0, %xmm0, %xmm14
	pshufd	$0, %xmm1, %xmm1
	subps	  %xmm1,   %xmm15
	unpcklps  %xmm1,   %xmm15

	movq	X, XX

	movq	M,  I
	sarq	$3, I
	jle	.L125

	movsd	(X), %xmm0
	addq	INCX, X
	movhps	(X), %xmm0
	addq	INCX, X

	movsd	(X), %xmm1
	addq	INCX, X
	movhps	(X), %xmm1
	addq	INCX, X

	movsd	(X), %xmm2
	addq	INCX, X
	movhps	(X), %xmm2
	addq	INCX, X

	movsd	(X), %xmm3
	addq	INCX, X
	movhps	(X), %xmm3
	addq	INCX, X

	decq	I
	jle	.L122
	ALIGN_4

.L121:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	pshufd	 $0xb1,  %xmm0, %xmm8
	mulps	 %xmm14, %xmm0
	mulps	 %xmm15, %xmm8
	addps	 %xmm8,  %xmm0

	movlps	%xmm0,  (XX)
	addq	INCX, XX
	movhps	%xmm0,  (XX)
	addq	INCX, XX

	movsd	(X), %xmm0
	addq	INCX, X
	movhps	(X), %xmm0
	addq	INCX, X

	pshufd	 $0xb1,  %xmm1, %xmm8
	mulps	 %xmm14, %xmm1
	mulps	 %xmm15, %xmm8
	addps	 %xmm8,  %xmm1

	movlps	%xmm1,  (XX)
	addq	INCX, XX
	movhps	%xmm1,  (XX)
	addq	INCX, XX

	movsd	(X), %xmm1
	addq	INCX, X
	movhps	(X), %xmm1
	addq	INCX, X

	pshufd	 $0xb1,  %xmm2, %xmm8
	mulps	 %xmm14, %xmm2
	mulps	 %xmm15, %xmm8
	addps	 %xmm8,  %xmm2

	movlps	%xmm2,  (XX)
	addq	INCX, XX
	movhps	%xmm2,  (XX)
	addq	INCX, XX

	movsd	(X), %xmm2
	addq	INCX, X
	movhps	(X), %xmm2
	addq	INCX, X

	pshufd	 $0xb1,  %xmm3, %xmm8
	mulps	 %xmm14, %xmm3
	mulps	 %xmm15, %xmm8
	addps	 %xmm8,  %xmm3

	movlps	%xmm3,  (XX)
	addq	INCX, XX
	movhps	%xmm3,  (XX)
	addq	INCX, XX

	movsd	(X), %xmm3
	addq	INCX, X
	movhps	(X), %xmm3
	addq	INCX, X

	decq	I
	jg	.L121
	ALIGN_4

.L122:
	pshufd	 $0xb1,  %xmm0, %xmm8
	mulps	 %xmm14, %xmm0
	mulps	 %xmm15, %xmm8
	addps	 %xmm8,  %xmm0

	movlps	%xmm0,  (XX)
	addq	INCX, XX
	movhps	%xmm0,  (XX)
	addq	INCX, XX

	pshufd	 $0xb1,  %xmm1, %xmm8
	mulps	 %xmm14, %xmm1
	mulps	 %xmm15, %xmm8
	addps	 %xmm8,  %xmm1

	movlps	%xmm1,  (XX)
	addq	INCX, XX
	movhps	%xmm1,  (XX)
	addq	INCX, XX

	pshufd	 $0xb1,  %xmm2, %xmm8
	mulps	 %xmm14, %xmm2
	mulps	 %xmm15, %xmm8
	addps	 %xmm8,  %xmm2

	movlps	%xmm2,  (XX)
	addq	INCX, XX
	movhps	%xmm2,  (XX)
	addq	INCX, XX

	pshufd	 $0xb1,  %xmm3, %xmm8
	mulps	 %xmm14, %xmm3
	mulps	 %xmm15, %xmm8
	addps	 %xmm8,  %xmm3

	movlps	%xmm3,  (XX)
	addq	INCX, XX
	movhps	%xmm3,  (XX)
	addq	INCX, XX
	ALIGN_4

.L125:
	testq	$4, M
	je	.L127

	movsd	(X), %xmm0
	addq	INCX, X
	movhps	(X), %xmm0
	addq	INCX, X

	pshufd	 $0xb1,  %xmm0, %xmm8
	mulps	 %xmm14, %xmm0
	mulps	 %xmm15, %xmm8
	addps	 %xmm8,  %xmm0

	movlps	%xmm0,  (XX)
	addq	INCX, XX
	movhps	%xmm0,  (XX)
	addq	INCX, XX

	movsd	(X), %xmm1
	addq	INCX, X
	movhps	(X), %xmm1
	addq	INCX, X

	pshufd	 $0xb1,  %xmm1, %xmm8
	mulps	 %xmm14, %xmm1
	mulps	 %xmm15, %xmm8
	addps	 %xmm8,  %xmm1

	movlps	%xmm1,  (XX)
	addq	INCX, XX
	movhps	%xmm1,  (XX)
	addq	INCX, XX
	ALIGN_3

.L127:
	testq	$2, M
	je	.L128

	movsd	(X), %xmm0
	addq	INCX, X
	movhps	(X), %xmm0
	addq	INCX, X

	pshufd	 $0xb1,  %xmm0, %xmm8
	mulps	 %xmm14, %xmm0
	mulps	 %xmm15, %xmm8
	addps	 %xmm8,  %xmm0

	movlps	%xmm0,  (XX)
	addq	INCX, XX
	movhps	%xmm0,  (XX)
	addq	INCX, XX
	ALIGN_3

.L128:
	testq	$1, M
	je	.L999

	movsd	(X), %xmm0

	pshufd	 $0xb1,  %xmm0, %xmm8
	mulps	 %xmm14, %xmm0
	mulps	 %xmm15, %xmm8
	addps	 %xmm8,  %xmm0

	movlps	%xmm0,  (XX)
	jmp	.L999
	ALIGN_3

.L130:
	cmpq	$2 * SIZE, INCX
	jne	.L120

#if defined(ALIGNED_ACCESS) && !defined(NEHALEM) && !defined(SANDYBRIDGE)

	pshufd	$0, %xmm0, %xmm14
	pshufd	$0, %xmm1, %xmm1
	subps	  %xmm1,   %xmm15
	unpcklps  %xmm1,   %xmm15

	subq	$-31 * SIZE, X

	testq	$2 * SIZE, X
	je	.L130x

	movsd	-31 * SIZE(X), %xmm0

	pshufd	 $0xb1,  %xmm0, %xmm8
	mulps	 %xmm14, %xmm0
	mulps	 %xmm15, %xmm8
	addps	 %xmm8,  %xmm0

	movlps	%xmm0,  -31 * SIZE(X)
	addq	$2 * SIZE, X
	decq	M
	jle	.L999
	ALIGN_3

.L130x:
	shufps	$0xb1, %xmm15, %xmm15

	movaps	-32 * SIZE(X), %xmm0
	movaps	%xmm0, %xmm9

	movq	M,  I
	sarq	$4, I
	jle	.L135

	movaps	-28 * SIZE(X), %xmm1
	movaps	-24 * SIZE(X), %xmm2
	movaps	-20 * SIZE(X), %xmm3
	movaps	-16 * SIZE(X), %xmm4
	movaps	-12 * SIZE(X), %xmm5
	movaps	 -8 * SIZE(X), %xmm6
	movaps	 -4 * SIZE(X), %xmm7

	decq	I
	jle	.L132
	ALIGN_4

.L131:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movss	%xmm1,  %xmm0
	pshufd	$0x1b,  %xmm0, %xmm8
	mulps	%xmm14, %xmm0
	mulps	%xmm15, %xmm8
	addps	%xmm8,  %xmm0
	movaps	%xmm0,  %xmm10
	movss	%xmm9,  %xmm0
	movaps	%xmm0,  -32 * SIZE(X)

	movaps	  0 * SIZE(X), %xmm0

	movss	%xmm2,  %xmm1
	pshufd	$0x1b,  %xmm1, %xmm8
	mulps	%xmm14, %xmm1
	mulps	%xmm15, %xmm8
	addps	%xmm8,  %xmm1
	movaps	%xmm1,  %xmm9
	movss	%xmm10,  %xmm1
	movaps	%xmm1,  -28 * SIZE(X)

	movaps	  4 * SIZE(X), %xmm1

	movss	%xmm3,  %xmm2
	pshufd	$0x1b,  %xmm2, %xmm8
	mulps	%xmm14, %xmm2
	mulps	%xmm15, %xmm8
	addps	%xmm8,  %xmm2
	movaps	%xmm2,  %xmm10
	movss	%xmm9,  %xmm2
	movaps	%xmm2,  -24 * SIZE(X)

	movaps	  8 * SIZE(X), %xmm2

	movss	%xmm4,  %xmm3
	pshufd	$0x1b,  %xmm3, %xmm8
	mulps	%xmm14, %xmm3
	mulps	%xmm15, %xmm8
	addps	%xmm8,  %xmm3
	movaps	%xmm3,  %xmm9
	movss	%xmm10,  %xmm3
	movaps	%xmm3,  -20 * SIZE(X)

	movaps	 12 * SIZE(X), %xmm3

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE + 64) - PREOFFSET(X)
#endif

	movss	%xmm5,  %xmm4
	pshufd	$0x1b,  %xmm4, %xmm8
	mulps	%xmm14, %xmm4
	mulps	%xmm15, %xmm8
	addps	%xmm8,  %xmm4
	movaps	%xmm4,  %xmm10
	movss	%xmm9,  %xmm4
	movaps	%xmm4,  -16 * SIZE(X)

	movaps	 16 * SIZE(X), %xmm4

	movss	%xmm6,  %xmm5
	pshufd	$0x1b,  %xmm5, %xmm8
	mulps	%xmm14, %xmm5
	mulps	%xmm15, %xmm8
	addps	%xmm8,  %xmm5
	movaps	%xmm5,  %xmm9
	movss	%xmm10,  %xmm5
	movaps	%xmm5,  -12 * SIZE(X)

	movaps	 20 * SIZE(X), %xmm5

	movss	%xmm7,  %xmm6
	pshufd	$0x1b,  %xmm6, %xmm8
	mulps	%xmm14, %xmm6
	mulps	%xmm15, %xmm8
	addps	%xmm8,  %xmm6
	movaps	%xmm6,  %xmm10
	movss	%xmm9,  %xmm6
	movaps	%xmm6,   -8 * SIZE(X)

	movaps	 24 * SIZE(X), %xmm6

	movss	%xmm0,  %xmm7
	pshufd	$0x1b,  %xmm7, %xmm8
	mulps	%xmm14, %xmm7
	mulps	%xmm15, %xmm8
	addps	%xmm8,  %xmm7
	movaps	%xmm7,  %xmm9
	movss	%xmm10,  %xmm7
	movaps	%xmm7,   -4 * SIZE(X)

	movaps	 28 * SIZE(X), %xmm7

	subq	$-32 * SIZE, X
	decq	I
	jg	.L131
	ALIGN_4

.L132:
	movss	%xmm1,  %xmm0
	pshufd	$0x1b,  %xmm0, %xmm8
	mulps	%xmm14, %xmm0
	mulps	%xmm15, %xmm8
	addps	%xmm8,  %xmm0
	movaps	%xmm0,  %xmm10
	movss	%xmm9,  %xmm0
	movaps	%xmm0,  -32 * SIZE(X)

	movaps	   0 * SIZE(X), %xmm0

	movss	%xmm2,  %xmm1
	pshufd	$0x1b,  %xmm1, %xmm8
	mulps	%xmm14, %xmm1
	mulps	%xmm15, %xmm8
	addps	%xmm8,  %xmm1
	movaps	%xmm1,  %xmm9
	movss	%xmm10,  %xmm1
	movaps	%xmm1,  -28 * SIZE(X)

	movss	%xmm3,  %xmm2
	pshufd	$0x1b,  %xmm2, %xmm8
	mulps	%xmm14, %xmm2
	mulps	%xmm15, %xmm8
	addps	%xmm8,  %xmm2
	movaps	%xmm2,  %xmm10
	movss	%xmm9,  %xmm2
	movaps	%xmm2,  -24 * SIZE(X)

	movss	%xmm4,  %xmm3
	pshufd	$0x1b,  %xmm3, %xmm8
	mulps	%xmm14, %xmm3
	mulps	%xmm15, %xmm8
	addps	%xmm8,  %xmm3
	movaps	%xmm3,  %xmm9
	movss	%xmm10,  %xmm3
	movaps	%xmm3,  -20 * SIZE(X)

	movss	%xmm5,  %xmm4
	pshufd	$0x1b,  %xmm4, %xmm8
	mulps	%xmm14, %xmm4
	mulps	%xmm15, %xmm8
	addps	%xmm8,  %xmm4
	movaps	%xmm4,  %xmm10
	movss	%xmm9,  %xmm4
	movaps	%xmm4,  -16 * SIZE(X)

	movss	%xmm6,  %xmm5
	pshufd	$0x1b,  %xmm5, %xmm8
	mulps	%xmm14, %xmm5
	mulps	%xmm15, %xmm8
	addps	%xmm8,  %xmm5
	movaps	%xmm5,  %xmm9
	movss	%xmm10,  %xmm5
	movaps	%xmm5,  -12 * SIZE(X)

	movss	%xmm7,  %xmm6
	pshufd	$0x1b,  %xmm6, %xmm8
	mulps	%xmm14, %xmm6
	mulps	%xmm15, %xmm8
	addps	%xmm8,  %xmm6
	movaps	%xmm6,  %xmm10
	movss	%xmm9,  %xmm6
	movaps	%xmm6,   -8 * SIZE(X)

	movss	%xmm0,  %xmm7
	pshufd	$0x1b,  %xmm7, %xmm8
	mulps	%xmm14, %xmm7
	mulps	%xmm15, %xmm8
	addps	%xmm8,  %xmm7
	movaps	%xmm7,  %xmm9
	movss	%xmm10,  %xmm7
	movaps	%xmm7,   -4 * SIZE(X)

	subq	$-32 * SIZE, X
	ALIGN_4

.L135:
	testq	$8, M
	je	.L136

	movaps	-28 * SIZE(X), %xmm1

	movss	%xmm1,  %xmm0
	pshufd	$0x1b,  %xmm0, %xmm8
	mulps	%xmm14, %xmm0
	mulps	%xmm15, %xmm8
	addps	%xmm8,  %xmm0

	movaps	%xmm0,  %xmm10
	movss	%xmm9,  %xmm0
	movaps	%xmm0,  -32 * SIZE(X)

	movaps	-24 * SIZE(X), %xmm2

	movss	%xmm2,  %xmm1
	pshufd	$0x1b,  %xmm1, %xmm8
	mulps	%xmm14, %xmm1
	mulps	%xmm15, %xmm8
	addps	%xmm8,  %xmm1

	movaps	%xmm1,  %xmm9
	movss	%xmm10,  %xmm1
	movaps	%xmm1,  -28 * SIZE(X)

	movaps	-20 * SIZE(X), %xmm3

	movss	%xmm3,  %xmm2
	pshufd	$0x1b,  %xmm2, %xmm8
	mulps	%xmm14, %xmm2
	mulps	%xmm15, %xmm8
	addps	%xmm8,  %xmm2

	movaps	%xmm2,  %xmm10
	movss	%xmm9,  %xmm2
	movaps	%xmm2,  -24 * SIZE(X)

	movaps	-16 * SIZE(X), %xmm0

	movss	%xmm0,  %xmm3
	pshufd	$0x1b,  %xmm3, %xmm8
	mulps	%xmm14, %xmm3
	mulps	%xmm15, %xmm8
	addps	%xmm8,  %xmm3

	movaps	%xmm3,  %xmm9
	movss	%xmm10,  %xmm3
	movaps	%xmm3,  -20 * SIZE(X)

	addq	$16 * SIZE, X
	ALIGN_3

.L136:
	testq	$4, M
	je	.L137

	movaps	-28 * SIZE(X), %xmm1

	movss	%xmm1,  %xmm0
	pshufd	$0x1b,  %xmm0, %xmm8
	mulps	%xmm14, %xmm0
	mulps	%xmm15, %xmm8
	addps	%xmm8,  %xmm0

	movaps	%xmm0,  %xmm10
	movss	%xmm9,  %xmm0
	movaps	%xmm0,  -32 * SIZE(X)

	movaps	-24 * SIZE(X), %xmm2

	movss	%xmm2,  %xmm1
	pshufd	$0x1b,  %xmm1, %xmm8
	mulps	%xmm14, %xmm1
	mulps	%xmm15, %xmm8
	addps	%xmm8,  %xmm1

	movaps	%xmm1,  %xmm9
	movss	%xmm10,  %xmm1
	movaps	%xmm1,  -28 * SIZE(X)

	movaps	%xmm2,  %xmm0

	addq	$8 * SIZE, X
	ALIGN_3

.L137:
	testq	$2, M
	je	.L138

	movaps	-28 * SIZE(X), %xmm1

	movss	%xmm1,  %xmm0
	pshufd	$0x1b,  %xmm0, %xmm8
	mulps	%xmm14, %xmm0
	mulps	%xmm15, %xmm8
	addps	%xmm8,  %xmm0

	movaps	%xmm0,  %xmm10
	movss	%xmm9,  %xmm0
	movaps	%xmm0,  -32 * SIZE(X)
	movaps	%xmm10, %xmm9
	movaps	%xmm1,  %xmm0

	addq	$4 * SIZE, X
	ALIGN_3

.L138:
	movss	%xmm9, -32 * SIZE(X)

	testq	$1, M
	je	.L999

	pshufd	 $0x1b,  %xmm0, %xmm8
	mulps	 %xmm14, %xmm0
	mulps	 %xmm15, %xmm8
	addps	 %xmm8,  %xmm0

	pshufd	 $0x39, %xmm0, %xmm0

	movlps	%xmm0,  -31 * SIZE(X)
	jmp	.L999
	ALIGN_3


#else

	pshufd	$0, %xmm0, %xmm14
	pshufd	$0, %xmm1, %xmm1
	subps	  %xmm1,   %xmm15
	unpcklps  %xmm1,   %xmm15

	subq	$-32 * SIZE, X

	testq	$2 * SIZE, X
	je	.L130x

	movsd	-32 * SIZE(X), %xmm0

	pshufd	 $0xb1,  %xmm0, %xmm8
	mulps	 %xmm14, %xmm0
	mulps	 %xmm15, %xmm8
	addps	 %xmm8,  %xmm0

	movlps	%xmm0,  -32 * SIZE(X)
	addq	$2 * SIZE, X
	decq	M
	jle	.L999
	ALIGN_3

.L130x:
	movq	M,  I
	sarq	$4, I
	jle	.L135

	movsd	-32 * SIZE(X), %xmm0
	movhps	-30 * SIZE(X), %xmm0
	movsd	-28 * SIZE(X), %xmm1
	movhps	-26 * SIZE(X), %xmm1
	movsd	-24 * SIZE(X), %xmm2
	movhps	-22 * SIZE(X), %xmm2
	movsd	-20 * SIZE(X), %xmm3
	movhps	-18 * SIZE(X), %xmm3
	movsd	-16 * SIZE(X), %xmm4
	movhps	-14 * SIZE(X), %xmm4
	movsd	-12 * SIZE(X), %xmm5
	movhps	-10 * SIZE(X), %xmm5
	movsd	 -8 * SIZE(X), %xmm6
	movhps	 -6 * SIZE(X), %xmm6
	movsd	 -4 * SIZE(X), %xmm7
	movhps	 -2 * SIZE(X), %xmm7

	decq	I
	jle	.L132
	ALIGN_4

.L131:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	pshufd	 $0xb1,  %xmm0, %xmm8
	mulps	 %xmm14, %xmm0
	mulps	 %xmm15, %xmm8
	addps	 %xmm8,  %xmm0
	movlps	%xmm0,  -32 * SIZE(X)
	movhps	%xmm0,  -30 * SIZE(X)

	movsd	  0 * SIZE(X), %xmm0
	movhps	  2 * SIZE(X), %xmm0

	pshufd	 $0xb1,  %xmm1, %xmm8
	mulps	 %xmm14, %xmm1
	mulps	 %xmm15, %xmm8
	addps	 %xmm8,  %xmm1
	movlps	%xmm1,  -28 * SIZE(X)
	movhps	%xmm1,  -26 * SIZE(X)

	movsd	  4 * SIZE(X), %xmm1
	movhps	  6 * SIZE(X), %xmm1

	pshufd	 $0xb1,  %xmm2, %xmm8
	mulps	 %xmm14, %xmm2
	mulps	 %xmm15, %xmm8
	addps	 %xmm8,  %xmm2
	movlps	%xmm2,  -24 * SIZE(X)
	movhps	%xmm2,  -22 * SIZE(X)

	movsd	  8 * SIZE(X), %xmm2
	movhps	 10 * SIZE(X), %xmm2

	pshufd	 $0xb1,  %xmm3, %xmm8
	mulps	 %xmm14, %xmm3
	mulps	 %xmm15, %xmm8
	addps	 %xmm8,  %xmm3
	movlps	%xmm3,  -20 * SIZE(X)
	movhps	%xmm3,  -18 * SIZE(X)

	movsd	 12 * SIZE(X), %xmm3
	movhps	 14 * SIZE(X), %xmm3

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE + 64) - PREOFFSET(X)
#endif

	pshufd	 $0xb1,  %xmm4, %xmm8
	mulps	 %xmm14, %xmm4
	mulps	 %xmm15, %xmm8
	addps	 %xmm8,  %xmm4
	movlps	%xmm4,  -16 * SIZE(X)
	movhps	%xmm4,  -14 * SIZE(X)

	movsd	 16 * SIZE(X), %xmm4
	movhps	 18 * SIZE(X), %xmm4

	pshufd	 $0xb1,  %xmm5, %xmm8
	mulps	 %xmm14, %xmm5
	mulps	 %xmm15, %xmm8
	addps	 %xmm8,  %xmm5
	movlps	%xmm5,  -12 * SIZE(X)
	movhps	%xmm5,  -10 * SIZE(X)

	movsd	 20 * SIZE(X), %xmm5
	movhps	 22 * SIZE(X), %xmm5

	pshufd	 $0xb1,  %xmm6, %xmm8
	mulps	 %xmm14, %xmm6
	mulps	 %xmm15, %xmm8
	addps	 %xmm8,  %xmm6
	movlps	%xmm6,   -8 * SIZE(X)
	movhps	%xmm6,   -6 * SIZE(X)

	movsd	 24 * SIZE(X), %xmm6
	movhps	 26 * SIZE(X), %xmm6

	pshufd	 $0xb1,  %xmm7, %xmm8
	mulps	 %xmm14, %xmm7
	mulps	 %xmm15, %xmm8
	addps	 %xmm8,  %xmm7
	movlps	%xmm7,   -4 * SIZE(X)
	movhps	%xmm7,   -2 * SIZE(X)

	movsd	 28 * SIZE(X), %xmm7
	movhps	 30 * SIZE(X), %xmm7

	subq	$-32 * SIZE, X
	decq	I
	jg	.L131
	ALIGN_4

.L132:
	pshufd	 $0xb1,  %xmm0, %xmm8
	mulps	 %xmm14, %xmm0
	mulps	 %xmm15, %xmm8
	addps	 %xmm8,  %xmm0
	movlps	%xmm0,  -32 * SIZE(X)
	movhps	%xmm0,  -30 * SIZE(X)

	pshufd	 $0xb1,  %xmm1, %xmm8
	mulps	 %xmm14, %xmm1
	mulps	 %xmm15, %xmm8
	addps	 %xmm8,  %xmm1
	movlps	%xmm1,  -28 * SIZE(X)
	movhps	%xmm1,  -26 * SIZE(X)

	pshufd	 $0xb1,  %xmm2, %xmm8
	mulps	 %xmm14, %xmm2
	mulps	 %xmm15, %xmm8
	addps	 %xmm8,  %xmm2
	movlps	%xmm2,  -24 * SIZE(X)
	movhps	%xmm2,  -22 * SIZE(X)

	pshufd	 $0xb1,  %xmm3, %xmm8
	mulps	 %xmm14, %xmm3
	mulps	 %xmm15, %xmm8
	addps	 %xmm8,  %xmm3
	movlps	%xmm3,  -20 * SIZE(X)
	movhps	%xmm3,  -18 * SIZE(X)

	pshufd	 $0xb1,  %xmm4, %xmm8
	mulps	 %xmm14, %xmm4
	mulps	 %xmm15, %xmm8
	addps	 %xmm8,  %xmm4
	movlps	%xmm4,  -16 * SIZE(X)
	movhps	%xmm4,  -14 * SIZE(X)

	pshufd	 $0xb1,  %xmm5, %xmm8
	mulps	 %xmm14, %xmm5
	mulps	 %xmm15, %xmm8
	addps	 %xmm8,  %xmm5
	movlps	%xmm5,  -12 * SIZE(X)
	movhps	%xmm5,  -10 * SIZE(X)

	pshufd	 $0xb1,  %xmm6, %xmm8
	mulps	 %xmm14, %xmm6
	mulps	 %xmm15, %xmm8
	addps	 %xmm8,  %xmm6
	movlps	%xmm6,   -8 * SIZE(X)
	movhps	%xmm6,   -6 * SIZE(X)

	pshufd	 $0xb1,  %xmm7, %xmm8
	mulps	 %xmm14, %xmm7
	mulps	 %xmm15, %xmm8
	addps	 %xmm8,  %xmm7
	movlps	%xmm7,   -4 * SIZE(X)
	movhps	%xmm7,   -2 * SIZE(X)

	subq	$-32 * SIZE, X
	ALIGN_4

.L135:
	testq	$8, M
	je	.L136

	movsd	-32 * SIZE(X), %xmm0
	movhps	-30 * SIZE(X), %xmm0

	pshufd	 $0xb1,  %xmm0, %xmm8
	mulps	 %xmm14, %xmm0
	mulps	 %xmm15, %xmm8
	addps	 %xmm8,  %xmm0
	movlps	%xmm0,  -32 * SIZE(X)
	movhps	%xmm0,  -30 * SIZE(X)

	movsd	-28 * SIZE(X), %xmm1
	movhps	-26 * SIZE(X), %xmm1

	pshufd	 $0xb1,  %xmm1, %xmm8
	mulps	 %xmm14, %xmm1
	mulps	 %xmm15, %xmm8
	addps	 %xmm8,  %xmm1
	movlps	%xmm1,  -28 * SIZE(X)
	movhps	%xmm1,  -26 * SIZE(X)

	movsd	-24 * SIZE(X), %xmm2
	movhps	-22 * SIZE(X), %xmm2

	pshufd	 $0xb1,  %xmm2, %xmm8
	mulps	 %xmm14, %xmm2
	mulps	 %xmm15, %xmm8
	addps	 %xmm8,  %xmm2
	movlps	%xmm2,  -24 * SIZE(X)
	movhps	%xmm2,  -22 * SIZE(X)

	movsd	-20 * SIZE(X), %xmm3
	movhps	-18 * SIZE(X), %xmm3

	pshufd	 $0xb1,  %xmm3, %xmm8
	mulps	 %xmm14, %xmm3
	mulps	 %xmm15, %xmm8
	addps	 %xmm8,  %xmm3
	movlps	%xmm3,  -20 * SIZE(X)
	movhps	%xmm3,  -18 * SIZE(X)

	addq	$16 * SIZE, X
	ALIGN_3

.L136:
	testq	$4, M
	je	.L137

	movsd	-32 * SIZE(X), %xmm0
	movhps	-30 * SIZE(X), %xmm0
	movsd	-28 * SIZE(X), %xmm1
	movhps	-26 * SIZE(X), %xmm1

	pshufd	 $0xb1,  %xmm0, %xmm8
	mulps	 %xmm14, %xmm0
	mulps	 %xmm15, %xmm8
	addps	 %xmm8,  %xmm0
	movlps	%xmm0,  -32 * SIZE(X)
	movhps	%xmm0,  -30 * SIZE(X)

	pshufd	 $0xb1,  %xmm1, %xmm8
	mulps	 %xmm14, %xmm1
	mulps	 %xmm15, %xmm8
	addps	 %xmm8,  %xmm1
	movlps	%xmm1,  -28 * SIZE(X)
	movhps	%xmm1,  -26 * SIZE(X)

	addq	$8 * SIZE, X
	ALIGN_3

.L137:
	testq	$2, M
	je	.L138

	movsd	-32 * SIZE(X), %xmm0
	movhps	-30 * SIZE(X), %xmm0

	pshufd	 $0xb1,  %xmm0, %xmm8
	mulps	 %xmm14, %xmm0
	mulps	 %xmm15, %xmm8
	addps	 %xmm8,  %xmm0
	movlps	%xmm0,  -32 * SIZE(X)
	movhps	%xmm0,  -30 * SIZE(X)

	addq	$4 * SIZE, X
	ALIGN_3

.L138:
	testq	$1, M
	je	.L999

	movsd	-32 * SIZE(X), %xmm0

	pshufd	 $0xb1,  %xmm0, %xmm8
	mulps	 %xmm14, %xmm0
	mulps	 %xmm15, %xmm8
	addps	 %xmm8,  %xmm0

	movlps	%xmm0,  -32 * SIZE(X)
	ALIGN_3
#endif

.L999:
	xorq	%rax, %rax

	RESTOREREGISTERS

	ret

	EPILOGUE
