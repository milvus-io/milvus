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

#if defined(NEHALEM) || defined(PENRYN) || defined(DUNNINGTON) || defined(BARCELONA) || defined(NANO)  || defined(SANDYBRIDGE)
#define  USE_PSHUFD
#else
#define  USE_PSHUFD_HALF
#endif

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
	comisd	%xmm0, %xmm15
	jne	.L100

	comisd	%xmm1, %xmm15
	jne	.L100

/* Alpha == ZERO */
	cmpq	$2 * SIZE, INCX
	jne	.L20

/* INCX == 1 */
	testq	$SIZE, X
	je	.L05

	movsd	%xmm15, 0 * SIZE(X)
	addq	$SIZE, X
	movq	$1, FLAG
	decq	M
	jle	.L19
	ALIGN_3
.L05:

	movq	M,  I	# rcx = n
	sarq	$3, I
	jle	.L12
	ALIGN_4

.L11:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movaps	%xmm15,  0 * SIZE(X)
	movaps	%xmm15,  2 * SIZE(X)
	movaps	%xmm15,  4 * SIZE(X)
	movaps	%xmm15,  6 * SIZE(X)

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE + 64) - PREOFFSET(X)
#endif

	movaps	%xmm15,  8 * SIZE(X)
	movaps	%xmm15, 10 * SIZE(X)
	movaps	%xmm15, 12 * SIZE(X)
	movaps	%xmm15, 14 * SIZE(X)

	addq	$16 * SIZE, X
	decq	I
	jg	.L11
	ALIGN_4

.L12:
	testq	$4, M
	je	.L13

	movaps	%xmm15,  0 * SIZE(X)
	movaps	%xmm15,  2 * SIZE(X)
	movaps	%xmm15,  4 * SIZE(X)
	movaps	%xmm15,  6 * SIZE(X)
	addq	$8 * SIZE, X
	ALIGN_3

.L13:
	testq  $2, M
	je    .L14

	movaps	%xmm15,  0 * SIZE(X)
	movaps	%xmm15,  2 * SIZE(X)
	addq	$4 * SIZE, X
	ALIGN_3

.L14:
	testq  $1, M
	je    .L19
	movaps	%xmm15,  0 * SIZE(X)
	addq	$2 * SIZE, X
	ALIGN_3

.L19:
	testq  $1, FLAG
	je    .L999

	movsd	%xmm15, 0 * SIZE(X)
	jmp	.L999
	ALIGN_4

/* incx != 1 */
.L20:
	testq	$SIZE, X
	jne	.L30

/* Aligned Mode */
	movq	M,  I		# rcx = n
	sarq	$2, I
	jle	.L22
	ALIGN_4

.L21:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movaps	%xmm15, (X)
	addq	INCX, X
	movaps	%xmm15, (X)
	addq	INCX, X
	movaps	%xmm15, (X)
	addq	INCX, X
	movaps	%xmm15, (X)
	addq	INCX, X
	decq	I
	jg	.L21
	ALIGN_4

.L22:
	testq	$3, M
	je	.L999

	testq	$2, M
	je	.L23

	movaps	%xmm15, (X)
	addq	INCX, X
	movaps	%xmm15, (X)
	addq	INCX, X
	ALIGN_3

.L23:
	testq	$1, M
	je	.L999

	movaps	%xmm15, (X)
	jmp	.L999
	ALIGN_4


/* Unaligned Mode */
.L30:
	movq	M,  I		# rcx = n
	sarq	$2, I
	jle	.L32
	ALIGN_4

.L31:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movlps	%xmm15, 0 * SIZE(X)
	movlps	%xmm15, 1 * SIZE(X)
	addq	INCX, X
	movlps	%xmm15, 0 * SIZE(X)
	movlps	%xmm15, 1 * SIZE(X)
	addq	INCX, X
	movlps	%xmm15, 0 * SIZE(X)
	movlps	%xmm15, 1 * SIZE(X)
	addq	INCX, X
	movlps	%xmm15, 0 * SIZE(X)
	movlps	%xmm15, 1 * SIZE(X)
	addq	INCX, X
	decq	I
	jg	.L31
	ALIGN_4

.L32:
	testq	$3, M
	je	.L999

	testq	$2, M
	je	.L33

	movlps	%xmm15, 0 * SIZE(X)
	movlps	%xmm15, 1 * SIZE(X)
	addq	INCX, X
	movlps	%xmm15, 0 * SIZE(X)
	movlps	%xmm15, 1 * SIZE(X)
	addq	INCX, X
	ALIGN_3

.L33:
	testq	$1, M
	je	.L999

	movlps	%xmm15, 0 * SIZE(X)
	movlps	%xmm15, 1 * SIZE(X)
	jmp	.L999
	ALIGN_4

/* Alpha != ZERO */
.L100:
	testq	$SIZE, X
	jne	.L200

#ifdef HAVE_SSE3
	movddup	%xmm0,  %xmm14
#else
	pshufd	$0x44, %xmm0, %xmm14
#endif
	pxor	%xmm15, %xmm15
	subsd	%xmm1,  %xmm15
	movlhps	%xmm1,  %xmm15

	cmpq	$2 * SIZE, INCX
	jne	.L120

	subq	$-16 * SIZE, X

	movq	M,  I
	sarq	$3, I
	jle	.L115

	movaps	-16 * SIZE(X), %xmm0
	movaps	-14 * SIZE(X), %xmm1
	movaps	-12 * SIZE(X), %xmm2
	movaps	-10 * SIZE(X), %xmm3
	movaps	 -8 * SIZE(X), %xmm4
	movaps	 -6 * SIZE(X), %xmm5
	movaps	 -4 * SIZE(X), %xmm6
	movaps	 -2 * SIZE(X), %xmm7

	decq	  I
	jle	  .L112
	ALIGN_4

.L111:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

#if defined(USE_PSHUFD) || defined(USE_PSHUFD_HALF)
	pshufd	$0x4e, %xmm0, %xmm8
#else
	movsd	 -15 * SIZE(X), %xmm8
	movhps	 -16 * SIZE(X), %xmm8
#endif
	mulpd	%xmm14, %xmm0
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm0
	movaps	%xmm0,  -16 * SIZE(X)
	movaps	  0 * SIZE(X), %xmm0

#ifdef USE_PSHUFD
	pshufd	$0x4e, %xmm1, %xmm8
#else
	movsd	 -13 * SIZE(X), %xmm8
	movhps	 -14 * SIZE(X), %xmm8
#endif
	mulpd	%xmm14, %xmm1
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm1
	movaps	%xmm1,  -14 * SIZE(X)
	movaps	  2 * SIZE(X), %xmm1

#if defined(USE_PSHUFD) || defined(USE_PSHUFD_HALF)
	pshufd	$0x4e, %xmm2, %xmm8
#else
	movsd	 -11 * SIZE(X), %xmm8
	movhps	 -12 * SIZE(X), %xmm8
#endif
	mulpd	%xmm14, %xmm2
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm2
	movaps	%xmm2,  -12 * SIZE(X)
	movaps	  4 * SIZE(X), %xmm2

#ifdef USE_PSHUFD
	pshufd	$0x4e, %xmm3, %xmm8
#else
	movsd	  -9 * SIZE(X), %xmm8
	movhps	 -10 * SIZE(X), %xmm8
#endif
	mulpd	%xmm14, %xmm3
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm3
	movaps	%xmm3,  -10 * SIZE(X)
	movaps	  6 * SIZE(X), %xmm3

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE + 64) - PREOFFSET(X)
#endif

#if defined(USE_PSHUFD) || defined(USE_PSHUFD_HALF)
	pshufd	$0x4e, %xmm4, %xmm8
#else
	movsd	  -7 * SIZE(X), %xmm8
	movhps	  -8 * SIZE(X), %xmm8
#endif
	mulpd	%xmm14, %xmm4
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm4
	movaps	%xmm4,   -8 * SIZE(X)
	movaps	  8 * SIZE(X), %xmm4

#ifdef USE_PSHUFD
	pshufd	$0x4e, %xmm5, %xmm8
#else
	movsd	  -5 * SIZE(X), %xmm8
	movhps	  -6 * SIZE(X), %xmm8
#endif
	mulpd	%xmm14, %xmm5
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm5
	movaps	%xmm5,   -6 * SIZE(X)
	movaps	 10 * SIZE(X), %xmm5

#if defined(USE_PSHUFD) || defined(USE_PSHUFD_HALF)
	pshufd	$0x4e, %xmm6, %xmm8
#else
	movsd	  -3 * SIZE(X), %xmm8
	movhps	  -4 * SIZE(X), %xmm8
#endif
	mulpd	%xmm14, %xmm6
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm6
	movaps	%xmm6,   -4 * SIZE(X)
	movaps	 12 * SIZE(X), %xmm6

#ifdef USE_PSHUFD
	pshufd	$0x4e, %xmm7, %xmm8
#else
	movsd	  -1 * SIZE(X), %xmm8
	movhps	  -2 * SIZE(X), %xmm8
#endif
	mulpd	%xmm14, %xmm7
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm7
	movaps	%xmm7,   -2 * SIZE(X)
	movaps	 14 * SIZE(X), %xmm7

	subq	$-16 * SIZE, X
	decq	I
	jg	.L111
	ALIGN_4

.L112:
	pshufd	$0x4e, %xmm0, %xmm8
	mulpd	%xmm14, %xmm0
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm0
	movaps	%xmm0,  -16 * SIZE(X)

	pshufd	$0x4e, %xmm1, %xmm8
	mulpd	%xmm14, %xmm1
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm1
	movaps	%xmm1,  -14 * SIZE(X)

	pshufd	$0x4e, %xmm2, %xmm8
	mulpd	%xmm14, %xmm2
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm2
	movaps	%xmm2,  -12 * SIZE(X)

	pshufd	$0x4e, %xmm3, %xmm8
	mulpd	%xmm14, %xmm3
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm3
	movaps	%xmm3,  -10 * SIZE(X)

	pshufd	$0x4e, %xmm4, %xmm8
	mulpd	%xmm14, %xmm4
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm4
	movaps	%xmm4,   -8 * SIZE(X)

	pshufd	$0x4e, %xmm5, %xmm8
	mulpd	%xmm14, %xmm5
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm5
	movaps	%xmm5,   -6 * SIZE(X)

	pshufd	$0x4e, %xmm6, %xmm8
	mulpd	%xmm14, %xmm6
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm6
	movaps	%xmm6,   -4 * SIZE(X)

	pshufd	$0x4e, %xmm7, %xmm8
	mulpd	%xmm14, %xmm7
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm7
	movaps	%xmm7,   -2 * SIZE(X)

	subq	$-16 * SIZE, X
	ALIGN_3

.L115:
	testq	$7, M
	je	.L999

	testq	$4, M
	je	.L116

	movaps	-16 * SIZE(X), %xmm0
	movaps	-14 * SIZE(X), %xmm1

	pshufd	$0x4e, %xmm0, %xmm8
	mulpd	%xmm14, %xmm0
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm0
	movaps	%xmm0,  -16 * SIZE(X)

	pshufd	$0x4e, %xmm1, %xmm8
	mulpd	%xmm14, %xmm1
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm1
	movaps	%xmm1,  -14 * SIZE(X)

	movaps	-12 * SIZE(X), %xmm2
	movaps	-10 * SIZE(X), %xmm3

	pshufd	$0x4e, %xmm2, %xmm8
	mulpd	%xmm14, %xmm2
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm2
	movaps	%xmm2,  -12 * SIZE(X)

	pshufd	$0x4e, %xmm3, %xmm8
	mulpd	%xmm14, %xmm3
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm3
	movaps	%xmm3,  -10 * SIZE(X)

	addq	$8 * SIZE, X
	ALIGN_3

.L116:
	testq	$2, M
	je	.L117

	movaps	-16 * SIZE(X), %xmm0
	movaps	-14 * SIZE(X), %xmm1

	pshufd	$0x4e, %xmm0, %xmm8
	mulpd	%xmm14, %xmm0
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm0
	movaps	%xmm0,  -16 * SIZE(X)

	pshufd	$0x4e, %xmm1, %xmm8
	mulpd	%xmm14, %xmm1
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm1
	movaps	%xmm1,  -14 * SIZE(X)

	addq	$4 * SIZE, X
	ALIGN_3

.L117:
	testq	$1, M
	je	.L999

	movaps	-16 * SIZE(X), %xmm0

	pshufd	$0x4e, %xmm0, %xmm8
	mulpd	%xmm14, %xmm0
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm0

	movaps	%xmm0,  -16 * SIZE(X)
	jmp	.L999
	ALIGN_3

.L120:
	movq	X, XX

	movq	M,  I
	sarq	$3, I
	jle	.L125

	movaps	(X), %xmm0
	addq	INCX, X
	movaps	(X), %xmm1
	addq	INCX, X
	movaps	(X), %xmm2
	addq	INCX, X
	movaps	(X), %xmm3
	addq	INCX, X
	movaps	(X), %xmm4
	addq	INCX, X
	movaps	(X), %xmm5
	addq	INCX, X
	movaps	(X), %xmm6
	addq	INCX, X
	movaps	(X), %xmm7
	addq	INCX, X

	decq	  I
	jle	  .L122
	ALIGN_4

.L121:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	pshufd	$0x4e, %xmm0, %xmm8
	mulpd	%xmm14, %xmm0
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm0
	movaps	%xmm0,  (XX)
	addq	INCX, XX
	movaps	(X), %xmm0
	addq	INCX, X

	pshufd	$0x4e, %xmm1, %xmm8
	mulpd	%xmm14, %xmm1
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm1
	movaps	%xmm1,  (XX)
	addq	INCX, XX
	movaps	(X), %xmm1
	addq	INCX, X

	pshufd	$0x4e, %xmm2, %xmm8
	mulpd	%xmm14, %xmm2
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm2
	movaps	%xmm2,  (XX)
	addq	INCX, XX
	movaps	(X), %xmm2
	addq	INCX, X

	pshufd	$0x4e, %xmm3, %xmm8
	mulpd	%xmm14, %xmm3
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm3
	movaps	%xmm3,  (XX)
	addq	INCX, XX
	movaps	(X), %xmm3
	addq	INCX, X

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE + 64) - PREOFFSET(X)
#endif

	pshufd	$0x4e, %xmm4, %xmm8
	mulpd	%xmm14, %xmm4
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm4
	movaps	%xmm4,  (XX)
	addq	INCX, XX
	movaps	(X), %xmm4
	addq	INCX, X

	pshufd	$0x4e, %xmm5, %xmm8
	mulpd	%xmm14, %xmm5
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm5
	movaps	%xmm5,  (XX)
	addq	INCX, XX
	movaps	(X), %xmm5
	addq	INCX, X

	pshufd	$0x4e, %xmm6, %xmm8
	mulpd	%xmm14, %xmm6
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm6
	movaps	%xmm6,  (XX)
	addq	INCX, XX
	movaps	(X), %xmm6
	addq	INCX, X

	pshufd	$0x4e, %xmm7, %xmm8
	mulpd	%xmm14, %xmm7
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm7
	movaps	%xmm7,  (XX)
	addq	INCX, XX
	movaps	(X), %xmm7
	addq	INCX, X

	decq	I
	jg	.L121
	ALIGN_4

.L122:
	pshufd	$0x4e, %xmm0, %xmm8
	mulpd	%xmm14, %xmm0
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm0
	movaps	%xmm0,  (XX)
	addq	INCX, XX

	pshufd	$0x4e, %xmm1, %xmm8
	mulpd	%xmm14, %xmm1
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm1
	movaps	%xmm1,  (XX)
	addq	INCX, XX

	pshufd	$0x4e, %xmm2, %xmm8
	mulpd	%xmm14, %xmm2
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm2
	movaps	%xmm2,  (XX)
	addq	INCX, XX

	pshufd	$0x4e, %xmm3, %xmm8
	mulpd	%xmm14, %xmm3
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm3
	movaps	%xmm3,  (XX)
	addq	INCX, XX

	pshufd	$0x4e, %xmm4, %xmm8
	mulpd	%xmm14, %xmm4
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm4
	movaps	%xmm4,  (XX)
	addq	INCX, XX

	pshufd	$0x4e, %xmm5, %xmm8
	mulpd	%xmm14, %xmm5
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm5
	movaps	%xmm5,  (XX)
	addq	INCX, XX

	pshufd	$0x4e, %xmm6, %xmm8
	mulpd	%xmm14, %xmm6
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm6
	movaps	%xmm6,  (XX)
	addq	INCX, XX

	pshufd	$0x4e, %xmm7, %xmm8
	mulpd	%xmm14, %xmm7
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm7
	movaps	%xmm7,  (XX)
	addq	INCX, XX
	ALIGN_3

.L125:
	testq	$7, M
	je	.L999

	testq	$4, M
	je	.L126

	movaps	(X), %xmm0
	addq	INCX, X
	movaps	(X), %xmm1
	addq	INCX, X

	pshufd	$0x4e, %xmm0, %xmm8
	mulpd	%xmm14, %xmm0
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm0
	movaps	%xmm0,  (XX)
	addq	INCX, XX

	pshufd	$0x4e, %xmm1, %xmm8
	mulpd	%xmm14, %xmm1
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm1
	movaps	%xmm1,  (XX)
	addq	INCX, XX

	movaps	(X), %xmm2
	addq	INCX, X
	movaps	(X), %xmm3
	addq	INCX, X

	pshufd	$0x4e, %xmm2, %xmm8
	mulpd	%xmm14, %xmm2
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm2
	movaps	%xmm2,  (XX)
	addq	INCX, XX

	pshufd	$0x4e, %xmm3, %xmm8
	mulpd	%xmm14, %xmm3
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm3
	movaps	%xmm3,  (XX)
	addq	INCX, XX
	ALIGN_3

.L126:
	testq	$2, M
	je	.L127

	movaps	(X), %xmm0
	addq	INCX, X
	movaps	(X), %xmm1
	addq	INCX, X

	pshufd	$0x4e, %xmm0, %xmm8
	mulpd	%xmm14, %xmm0
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm0
	movaps	%xmm0,  (XX)
	addq	INCX, XX

	pshufd	$0x4e, %xmm1, %xmm8
	mulpd	%xmm14, %xmm1
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm1
	movaps	%xmm1,  (XX)
	addq	INCX, XX
	ALIGN_3

.L127:
	testq	$1, M
	je	.L999

	movaps	(X), %xmm0

	pshufd	$0x4e, %xmm0, %xmm8
	mulpd	%xmm14, %xmm0
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm0

	movaps	%xmm0,  (XX)
	jmp	.L999
	ALIGN_3

.L200:
	cmpq	$2 * SIZE, INCX
	jne	.L220

#if defined(ALIGNED_ACCESS) && !defined(NEHALEM) && !defined(SANDYBRIDGE)

	movddup	%xmm0,  %xmm14
	pxor	%xmm15, %xmm15
	subsd	%xmm1,  %xmm15
	movlhps	%xmm1,  %xmm15
	shufpd	$1, %xmm15, %xmm15

	movhps	0 * SIZE(X), %xmm0
	movaps	1 * SIZE(X), %xmm1
	subq	$-16 * SIZE, X

	unpckhpd %xmm0, %xmm0
	mulsd	%xmm14, %xmm0
	movaps	%xmm1,  %xmm8
	mulsd	%xmm15, %xmm8
	subsd	%xmm8,  %xmm0
	movlps	%xmm0, -16 * SIZE(X)

	decq	M

	movq	M,  I
	sarq	$3, I
	jle	.L205

	movaps	-13 * SIZE(X), %xmm2
	movaps	-11 * SIZE(X), %xmm3
	movaps	 -9 * SIZE(X), %xmm4

	decq	  I
	jle	  .L202
	ALIGN_4

.L201:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movaps	%xmm1,  %xmm8
	SHUFPD_1 %xmm2, %xmm0
	mulpd	%xmm14, %xmm8
	mulpd	%xmm15, %xmm0
	addpd	%xmm8,  %xmm0
	movaps	%xmm0,  -15 * SIZE(X)
	movaps	 -7 * SIZE(X), %xmm5

	movaps	%xmm2,  %xmm8
	SHUFPD_1 %xmm3, %xmm1
	mulpd	%xmm14, %xmm8
	mulpd	%xmm15, %xmm1
	addpd	%xmm8,  %xmm1
	movaps	%xmm1,  -13 * SIZE(X)
	movaps	 -5 * SIZE(X), %xmm6

	movaps	%xmm3,  %xmm8
	SHUFPD_1 %xmm4, %xmm2
	mulpd	%xmm14, %xmm8
	mulpd	%xmm15, %xmm2
	addpd	%xmm8,  %xmm2
	movaps	%xmm2,  -11 * SIZE(X)
	movaps	 -3 * SIZE(X), %xmm7

	movaps	%xmm4,  %xmm8
	SHUFPD_1 %xmm5, %xmm3
	mulpd	%xmm14, %xmm8
	mulpd	%xmm15, %xmm3
	addpd	%xmm8,  %xmm3
	movaps	%xmm3,   -9 * SIZE(X)
 	movaps	 -1 * SIZE(X), %xmm0

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE + 64) - PREOFFSET(X)
#endif

	movaps	%xmm5,  %xmm8
	SHUFPD_1 %xmm6, %xmm4
	mulpd	%xmm14, %xmm8
	mulpd	%xmm15, %xmm4
	addpd	%xmm8,  %xmm4
	movaps	%xmm4,   -7 * SIZE(X)
	movaps	  1 * SIZE(X), %xmm1

	movaps	%xmm6,  %xmm8
	SHUFPD_1 %xmm7, %xmm5
	mulpd	%xmm14, %xmm8
	mulpd	%xmm15, %xmm5
	addpd	%xmm8,  %xmm5
	movaps	%xmm5,   -5 * SIZE(X)
	movaps	  3 * SIZE(X), %xmm2

	movaps	%xmm7,  %xmm8
	SHUFPD_1 %xmm0, %xmm6
	mulpd	%xmm14, %xmm8
	mulpd	%xmm15, %xmm6
	addpd	%xmm8,  %xmm6
	movaps	%xmm6,   -3 * SIZE(X)
	movaps	  5 * SIZE(X), %xmm3

	movaps	%xmm0,  %xmm8
	SHUFPD_1 %xmm1, %xmm7
	mulpd	%xmm14, %xmm8
	mulpd	%xmm15, %xmm7
	addpd	%xmm8,  %xmm7
	movaps	%xmm7,   -1 * SIZE(X)
	movaps	  7 * SIZE(X), %xmm4

	subq	$-16 * SIZE, X
	decq	I
	jg	.L201
	ALIGN_4

.L202:
	movaps	%xmm1,  %xmm8
	SHUFPD_1 %xmm2, %xmm0
	mulpd	%xmm14, %xmm8
	mulpd	%xmm15, %xmm0
	addpd	%xmm8,  %xmm0
	movaps	%xmm0,  -15 * SIZE(X)
	movaps	 -7 * SIZE(X), %xmm5

	movaps	%xmm2,  %xmm8
	SHUFPD_1 %xmm3, %xmm1
	mulpd	%xmm14, %xmm8
	mulpd	%xmm15, %xmm1
	addpd	%xmm8,  %xmm1
	movaps	%xmm1,  -13 * SIZE(X)
	movaps	 -5 * SIZE(X), %xmm6

	movaps	%xmm3,  %xmm8
	SHUFPD_1 %xmm4, %xmm2
	mulpd	%xmm14, %xmm8
	mulpd	%xmm15, %xmm2
	addpd	%xmm8,  %xmm2
	movaps	%xmm2,  -11 * SIZE(X)
	movaps	 -3 * SIZE(X), %xmm7

	movaps	%xmm4,  %xmm8
	SHUFPD_1 %xmm5, %xmm3
	mulpd	%xmm14, %xmm8
	mulpd	%xmm15, %xmm3
	addpd	%xmm8,  %xmm3
	movaps	%xmm3,   -9 * SIZE(X)
	movaps	 -1 * SIZE(X), %xmm0

	movaps	%xmm5,  %xmm8
	SHUFPD_1 %xmm6, %xmm4
	mulpd	%xmm14, %xmm8
	mulpd	%xmm15, %xmm4
	addpd	%xmm8,  %xmm4
	movaps	%xmm4,   -7 * SIZE(X)
	movaps	  1 * SIZE(X), %xmm1

	movaps	%xmm6,  %xmm8
	SHUFPD_1 %xmm7, %xmm5
	mulpd	%xmm14, %xmm8
	mulpd	%xmm15, %xmm5
	addpd	%xmm8,  %xmm5
	movaps	%xmm5,   -5 * SIZE(X)

	movaps	%xmm7,  %xmm8
	SHUFPD_1 %xmm0, %xmm6
	mulpd	%xmm14, %xmm8
	mulpd	%xmm15, %xmm6
	addpd	%xmm8,  %xmm6
	movaps	%xmm6,   -3 * SIZE(X)

	movaps	%xmm0,  %xmm8
	SHUFPD_1 %xmm1, %xmm7
	mulpd	%xmm14, %xmm8
	mulpd	%xmm15, %xmm7
	addpd	%xmm8,  %xmm7
	movaps	%xmm7,   -1 * SIZE(X)

	subq	$-16 * SIZE, X
	ALIGN_3

.L205:
	testq	$4, M
	je	.L206

	movaps	-13 * SIZE(X), %xmm2

	movaps	%xmm1,  %xmm8
	SHUFPD_1 %xmm2, %xmm0
	mulpd	%xmm14, %xmm8
	mulpd	%xmm15, %xmm0
	addpd	%xmm8,  %xmm0
	movaps	%xmm0,  -15 * SIZE(X)

	movaps	-11 * SIZE(X), %xmm3

	movaps	%xmm2,  %xmm8
	SHUFPD_1 %xmm3, %xmm1
	mulpd	%xmm14, %xmm8
	mulpd	%xmm15, %xmm1
	addpd	%xmm8,  %xmm1
	movaps	%xmm1,  -13 * SIZE(X)

	movaps	 -9 * SIZE(X), %xmm0

	movaps	%xmm3,  %xmm8
	SHUFPD_1 %xmm0, %xmm2
	mulpd	%xmm14, %xmm8
	mulpd	%xmm15, %xmm2
	addpd	%xmm8,  %xmm2
	movaps	%xmm2,  -11 * SIZE(X)

	movaps	 -7 * SIZE(X), %xmm1

	movaps	%xmm0,  %xmm8
	SHUFPD_1 %xmm1, %xmm3
	mulpd	%xmm14, %xmm8
	mulpd	%xmm15, %xmm3
	addpd	%xmm8,  %xmm3
	movaps	%xmm3,   -9 * SIZE(X)

	addq	$8 * SIZE, X
	ALIGN_3

.L206:
	testq	$2, M
	je	.L207

	movaps	-13 * SIZE(X), %xmm2

	movaps	%xmm1,  %xmm8
	SHUFPD_1 %xmm2, %xmm0

	mulpd	%xmm14, %xmm8
	mulpd	%xmm15, %xmm0
	addpd	%xmm8,  %xmm0
	movaps	%xmm0,  -15 * SIZE(X)

	movaps	-11 * SIZE(X), %xmm3

	movaps	%xmm2,  %xmm8
	SHUFPD_1 %xmm3, %xmm1

	mulpd	%xmm14, %xmm8
	mulpd	%xmm15, %xmm1
	addpd	%xmm8,  %xmm1
	movaps	%xmm1,  -13 * SIZE(X)

	movaps	%xmm2, %xmm0
	movaps	%xmm3, %xmm1

	addq	$4 * SIZE, X
	ALIGN_3

.L207:
	testq	$1, M
	je	.L208

	movaps	-13 * SIZE(X), %xmm2

	movaps	%xmm1,  %xmm8
	SHUFPD_1 %xmm2, %xmm0

	mulpd	%xmm14, %xmm8
	mulpd	%xmm15, %xmm0
	addpd	%xmm8,  %xmm0
	movaps	%xmm0,  -15 * SIZE(X)

	movaps	%xmm1, %xmm0
	movaps	%xmm2, %xmm1
	addq	$2 * SIZE, X
	ALIGN_3

.L208:
	unpckhpd %xmm0, %xmm0
	mulsd	%xmm14, %xmm1
	mulsd	%xmm15, %xmm0
	addsd	%xmm1,  %xmm0
	movlps	%xmm0, -15 * SIZE(X)
	jmp	.L999
	ALIGN_3

#else

	movddup	%xmm0,  %xmm14
	pxor	%xmm15, %xmm15
	subsd	%xmm1,  %xmm15
	movlhps	%xmm1,  %xmm15

	subq	$-16 * SIZE, X

	movq	M,  I
	sarq	$3, I
	jle	.L205

	movsd	-16 * SIZE(X), %xmm0
	movhps	-15 * SIZE(X), %xmm0
	movsd	-14 * SIZE(X), %xmm1
	movhps	-13 * SIZE(X), %xmm1
	movsd	-12 * SIZE(X), %xmm2
	movhps	-11 * SIZE(X), %xmm2
	movsd	-10 * SIZE(X), %xmm3
	movhps	 -9 * SIZE(X), %xmm3

	movsd	 -8 * SIZE(X), %xmm4
	movhps	 -7 * SIZE(X), %xmm4
	movsd	 -6 * SIZE(X), %xmm5
	movhps	 -5 * SIZE(X), %xmm5
	movsd	 -4 * SIZE(X), %xmm6
	movhps	 -3 * SIZE(X), %xmm6
	movsd	 -2 * SIZE(X), %xmm7
	movhps	 -1 * SIZE(X), %xmm7

	decq	  I
	jle	  .L202
	ALIGN_4

.L201:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

#if defined(USE_PSHUFD) || defined(USE_PSHUFD_HALF)
	pshufd	$0x4e, %xmm0, %xmm8
#else
	movsd	 -15 * SIZE(X), %xmm8
	movhps	 -16 * SIZE(X), %xmm8
#endif
	mulpd	%xmm14, %xmm0
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm0
	movlps	%xmm0,  -16 * SIZE(X)
	movhps	%xmm0,  -15 * SIZE(X)
	movsd	  0 * SIZE(X), %xmm0
	movhps	  1 * SIZE(X), %xmm0

#ifdef USE_PSHUFD
	pshufd	$0x4e, %xmm1, %xmm8
#else
	movsd	 -13 * SIZE(X), %xmm8
	movhps	 -14 * SIZE(X), %xmm8
#endif
	mulpd	%xmm14, %xmm1
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm1
	movlps	%xmm1,  -14 * SIZE(X)
	movhps	%xmm1,  -13 * SIZE(X)
	movsd	  2 * SIZE(X), %xmm1
	movhps	  3 * SIZE(X), %xmm1

#if defined(USE_PSHUFD) || defined(USE_PSHUFD_HALF)
	pshufd	$0x4e, %xmm2, %xmm8
#else
	movsd	 -11 * SIZE(X), %xmm8
	movhps	 -12 * SIZE(X), %xmm8
#endif
	mulpd	%xmm14, %xmm2
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm2
	movlps	%xmm2,  -12 * SIZE(X)
	movhps	%xmm2,  -11 * SIZE(X)
	movsd	  4 * SIZE(X), %xmm2
	movhps	  5 * SIZE(X), %xmm2

#ifdef USE_PSHUFD
	pshufd	$0x4e, %xmm3, %xmm8
#else
	movsd	  -9 * SIZE(X), %xmm8
	movhps	 -10 * SIZE(X), %xmm8
#endif
	mulpd	%xmm14, %xmm3
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm3
	movlps	%xmm3,  -10 * SIZE(X)
	movhps	%xmm3,   -9 * SIZE(X)
	movsd	  6 * SIZE(X), %xmm3
	movhps	  7 * SIZE(X), %xmm3

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE + 64) - PREOFFSET(X)
#endif

#if defined(USE_PSHUFD) || defined(USE_PSHUFD_HALF)
	pshufd	$0x4e, %xmm4, %xmm8
#else
	movsd	  -7 * SIZE(X), %xmm8
	movhps	  -8 * SIZE(X), %xmm8
#endif
	mulpd	%xmm14, %xmm4
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm4
	movlps	%xmm4,   -8 * SIZE(X)
	movhps	%xmm4,   -7 * SIZE(X)
	movsd	  8 * SIZE(X), %xmm4
	movhps	  9 * SIZE(X), %xmm4

#ifdef USE_PSHUFD
	pshufd	$0x4e, %xmm5, %xmm8
#else
	movsd	  -5 * SIZE(X), %xmm8
	movhps	  -6 * SIZE(X), %xmm8
#endif
	mulpd	%xmm14, %xmm5
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm5
	movlps	%xmm5,   -6 * SIZE(X)
	movhps	%xmm5,   -5 * SIZE(X)
	movsd	 10 * SIZE(X), %xmm5
	movhps	 11 * SIZE(X), %xmm5

#if defined(USE_PSHUFD) || defined(USE_PSHUFD_HALF)
	pshufd	$0x4e, %xmm6, %xmm8
#else
	movsd	  -3 * SIZE(X), %xmm8
	movhps	  -4 * SIZE(X), %xmm8
#endif
	mulpd	%xmm14, %xmm6
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm6
	movlps	%xmm6,   -4 * SIZE(X)
	movhps	%xmm6,   -3 * SIZE(X)
	movsd	 12 * SIZE(X), %xmm6
	movhps	 13 * SIZE(X), %xmm6

#ifdef USE_PSHUFD
	pshufd	$0x4e, %xmm7, %xmm8
#else
	movsd	  -1 * SIZE(X), %xmm8
	movhps	  -2 * SIZE(X), %xmm8
#endif
	mulpd	%xmm14, %xmm7
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm7
	movlps	%xmm7,   -2 * SIZE(X)
	movhps	%xmm7,   -1 * SIZE(X)
	movsd	 14 * SIZE(X), %xmm7
	movhps	 15 * SIZE(X), %xmm7

	subq	$-16 * SIZE, X
	decq	I
	jg	.L201
	ALIGN_4

.L202:
	pshufd	$0x4e, %xmm0, %xmm8
	mulpd	%xmm14, %xmm0
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm0
	movlps	%xmm0,  -16 * SIZE(X)
	movhps	%xmm0,  -15 * SIZE(X)

	pshufd	$0x4e, %xmm1, %xmm8
	mulpd	%xmm14, %xmm1
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm1
	movlps	%xmm1,  -14 * SIZE(X)
	movhps	%xmm1,  -13 * SIZE(X)

	pshufd	$0x4e, %xmm2, %xmm8
	mulpd	%xmm14, %xmm2
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm2
	movlps	%xmm2,  -12 * SIZE(X)
	movhps	%xmm2,  -11 * SIZE(X)

	pshufd	$0x4e, %xmm3, %xmm8
	mulpd	%xmm14, %xmm3
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm3
	movlps	%xmm3,  -10 * SIZE(X)
	movhps	%xmm3,   -9 * SIZE(X)

	pshufd	$0x4e, %xmm4, %xmm8
	mulpd	%xmm14, %xmm4
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm4
	movlps	%xmm4,   -8 * SIZE(X)
	movhps	%xmm4,   -7 * SIZE(X)

	pshufd	$0x4e, %xmm5, %xmm8
	mulpd	%xmm14, %xmm5
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm5
	movlps	%xmm5,   -6 * SIZE(X)
	movhps	%xmm5,   -5 * SIZE(X)

	pshufd	$0x4e, %xmm6, %xmm8
	mulpd	%xmm14, %xmm6
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm6
	movlps	%xmm6,   -4 * SIZE(X)
	movhps	%xmm6,   -3 * SIZE(X)

	pshufd	$0x4e, %xmm7, %xmm8
	mulpd	%xmm14, %xmm7
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm7
	movlps	%xmm7,   -2 * SIZE(X)
	movhps	%xmm7,   -1 * SIZE(X)

	subq	$-16 * SIZE, X
	ALIGN_3

.L205:
	testq	$7, M
	je	.L999

	testq	$4, M
	je	.L206

	movsd	-16 * SIZE(X), %xmm0
	movhps	-15 * SIZE(X), %xmm0
	movsd	-14 * SIZE(X), %xmm1
	movhps	-13 * SIZE(X), %xmm1

	pshufd	$0x4e, %xmm0, %xmm8
	mulpd	%xmm14, %xmm0
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm0
	movlps	%xmm0,  -16 * SIZE(X)
	movhps	%xmm0,  -15 * SIZE(X)

	pshufd	$0x4e, %xmm1, %xmm8
	mulpd	%xmm14, %xmm1
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm1
	movlps	%xmm1,  -14 * SIZE(X)
	movhps	%xmm1,  -13 * SIZE(X)

	movsd	-12 * SIZE(X), %xmm2
	movhps	-11 * SIZE(X), %xmm2
	movsd	-10 * SIZE(X), %xmm3
	movhps	 -9 * SIZE(X), %xmm3

	pshufd	$0x4e, %xmm2, %xmm8
	mulpd	%xmm14, %xmm2
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm2
	movlps	%xmm2,  -12 * SIZE(X)
	movhps	%xmm2,  -11 * SIZE(X)

	pshufd	$0x4e, %xmm3, %xmm8
	mulpd	%xmm14, %xmm3
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm3
	movlps	%xmm3,  -10 * SIZE(X)
	movhps	%xmm3,   -9 * SIZE(X)

	addq	$8 * SIZE, X
	ALIGN_3

.L206:
	testq	$2, M
	je	.L207

	movsd	-16 * SIZE(X), %xmm0
	movhps	-15 * SIZE(X), %xmm0

	pshufd	$0x4e, %xmm0, %xmm8
	mulpd	%xmm14, %xmm0
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm0
	movlps	%xmm0,  -16 * SIZE(X)
	movhps	%xmm0,  -15 * SIZE(X)

	movsd	-14 * SIZE(X), %xmm1
	movhps	-13 * SIZE(X), %xmm1

	pshufd	$0x4e, %xmm1, %xmm8
	mulpd	%xmm14, %xmm1
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm1
	movlps	%xmm1,  -14 * SIZE(X)
	movhps	%xmm1,  -13 * SIZE(X)

	addq	$4 * SIZE, X
	ALIGN_3

.L207:
	testq	$1, M
	je	.L999

	movsd	-16 * SIZE(X), %xmm0
	movhps	-15 * SIZE(X), %xmm0

	pshufd	$0x4e, %xmm0, %xmm8
	mulpd	%xmm14, %xmm0
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm0

	movlps	%xmm0,  -16 * SIZE(X)
	movhps	%xmm0,  -15 * SIZE(X)
	jmp	.L999
	ALIGN_3

#endif

.L220:
	movddup	%xmm0,  %xmm14
	pxor	%xmm15, %xmm15
	subsd	%xmm1,  %xmm15
	movlhps	%xmm1,  %xmm15

	movq	X, XX

	movq	M,  I
	sarq	$3, I
	jle	.L225

	movsd	 0 * SIZE(X), %xmm0
	movhps	 1 * SIZE(X), %xmm0
	addq	 INCX, X
	movsd	 0 * SIZE(X), %xmm1
	movhps	 1 * SIZE(X), %xmm1
	addq	 INCX, X
	movsd	 0 * SIZE(X), %xmm2
	movhps	 1 * SIZE(X), %xmm2
	addq	 INCX, X
	movsd	 0 * SIZE(X), %xmm3
	movhps	 1 * SIZE(X), %xmm3
	addq	 INCX, X
	movsd	 0 * SIZE(X), %xmm4
	movhps	 1 * SIZE(X), %xmm4
	addq	 INCX, X
	movsd	 0 * SIZE(X), %xmm5
	movhps	 1 * SIZE(X), %xmm5
	addq	 INCX, X
	movsd	 0 * SIZE(X), %xmm6
	movhps	 1 * SIZE(X), %xmm6
	addq	 INCX, X
	movsd	 0 * SIZE(X), %xmm7
	movhps	 1 * SIZE(X), %xmm7
	addq	 INCX, X

	decq	  I
	jle	  .L222
	ALIGN_4

.L221:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	pshufd	$0x4e, %xmm0, %xmm8
	mulpd	%xmm14, %xmm0
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm0
	movlps	%xmm0,   0 * SIZE(XX)
	movhps	%xmm0,   1 * SIZE(XX)
	addq	 INCX, XX

	movsd	 0 * SIZE(X), %xmm0
	movhps	 1 * SIZE(X), %xmm0
	addq	 INCX, X

	pshufd	$0x4e, %xmm1, %xmm8
	mulpd	%xmm14, %xmm1
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm1
	movlps	%xmm1,   0 * SIZE(XX)
	movhps	%xmm1,   1 * SIZE(XX)
	addq	 INCX, XX

	movsd	 0 * SIZE(X), %xmm1
	movhps	 1 * SIZE(X), %xmm1
	addq	 INCX, X

	pshufd	$0x4e, %xmm2, %xmm8
	mulpd	%xmm14, %xmm2
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm2
	movlps	%xmm2,   0 * SIZE(XX)
	movhps	%xmm2,   1 * SIZE(XX)
	addq	 INCX, XX

	movsd	 0 * SIZE(X), %xmm2
	movhps	 1 * SIZE(X), %xmm2
	addq	 INCX, X

	pshufd	$0x4e, %xmm3, %xmm8
	mulpd	%xmm14, %xmm3
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm3
	movlps	%xmm3,   0 * SIZE(XX)
	movhps	%xmm3,   1 * SIZE(XX)
	addq	 INCX, XX

	movsd	 0 * SIZE(X), %xmm3
	movhps	 1 * SIZE(X), %xmm3
	addq	 INCX, X

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE + 64) - PREOFFSET(X)
#endif

	pshufd	$0x4e, %xmm4, %xmm8
	mulpd	%xmm14, %xmm4
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm4
	movlps	%xmm4,   0 * SIZE(XX)
	movhps	%xmm4,   1 * SIZE(XX)
	addq	 INCX, XX

	movsd	 0 * SIZE(X), %xmm4
	movhps	 1 * SIZE(X), %xmm4
	addq	 INCX, X

	pshufd	$0x4e, %xmm5, %xmm8
	mulpd	%xmm14, %xmm5
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm5
	movlps	%xmm5,   0 * SIZE(XX)
	movhps	%xmm5,   1 * SIZE(XX)
	addq	 INCX, XX

	movsd	 0 * SIZE(X), %xmm5
	movhps	 1 * SIZE(X), %xmm5
	addq	 INCX, X

	pshufd	$0x4e, %xmm6, %xmm8
	mulpd	%xmm14, %xmm6
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm6
	movlps	%xmm6,   0 * SIZE(XX)
	movhps	%xmm6,   1 * SIZE(XX)
	addq	 INCX, XX

	movsd	 0 * SIZE(X), %xmm6
	movhps	 1 * SIZE(X), %xmm6
	addq	 INCX, X

	pshufd	$0x4e, %xmm7, %xmm8
	mulpd	%xmm14, %xmm7
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm7
	movlps	%xmm7,   0 * SIZE(XX)
	movhps	%xmm7,   1 * SIZE(XX)
	addq	 INCX, XX

	movsd	 0 * SIZE(X), %xmm7
	movhps	 1 * SIZE(X), %xmm7
	addq	 INCX, X


	decq	I
	jg	.L221
	ALIGN_4

.L222:
	pshufd	$0x4e, %xmm0, %xmm8
	mulpd	%xmm14, %xmm0
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm0
	movlps	%xmm0,   0 * SIZE(XX)
	movhps	%xmm0,   1 * SIZE(XX)
	addq	 INCX, XX

	pshufd	$0x4e, %xmm1, %xmm8
	mulpd	%xmm14, %xmm1
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm1
	movlps	%xmm1,   0 * SIZE(XX)
	movhps	%xmm1,   1 * SIZE(XX)
	addq	 INCX, XX

	pshufd	$0x4e, %xmm2, %xmm8
	mulpd	%xmm14, %xmm2
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm2
	movlps	%xmm2,   0 * SIZE(XX)
	movhps	%xmm2,   1 * SIZE(XX)
	addq	 INCX, XX

	pshufd	$0x4e, %xmm3, %xmm8
	mulpd	%xmm14, %xmm3
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm3
	movlps	%xmm3,   0 * SIZE(XX)
	movhps	%xmm3,   1 * SIZE(XX)
	addq	 INCX, XX

	pshufd	$0x4e, %xmm4, %xmm8
	mulpd	%xmm14, %xmm4
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm4
	movlps	%xmm4,   0 * SIZE(XX)
	movhps	%xmm4,   1 * SIZE(XX)
	addq	 INCX, XX

	pshufd	$0x4e, %xmm5, %xmm8
	mulpd	%xmm14, %xmm5
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm5
	movlps	%xmm5,   0 * SIZE(XX)
	movhps	%xmm5,   1 * SIZE(XX)
	addq	 INCX, XX

	pshufd	$0x4e, %xmm6, %xmm8
	mulpd	%xmm14, %xmm6
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm6
	movlps	%xmm6,   0 * SIZE(XX)
	movhps	%xmm6,   1 * SIZE(XX)
	addq	 INCX, XX

	pshufd	$0x4e, %xmm7, %xmm8
	mulpd	%xmm14, %xmm7
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm7
	movlps	%xmm7,   0 * SIZE(XX)
	movhps	%xmm7,   1 * SIZE(XX)
	addq	 INCX, XX
	ALIGN_3

.L225:
	testq	$7, M
	je	.L999

	testq	$4, M
	je	.L226

	movsd	 0 * SIZE(X), %xmm0
	movhps	 1 * SIZE(X), %xmm0
	addq	 INCX, X

	pshufd	$0x4e, %xmm0, %xmm8
	mulpd	%xmm14, %xmm0
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm0
	movlps	%xmm0,   0 * SIZE(XX)
	movhps	%xmm0,   1 * SIZE(XX)
	addq	 INCX, XX

	movsd	 0 * SIZE(X), %xmm1
	movhps	 1 * SIZE(X), %xmm1
	addq	 INCX, X

	pshufd	$0x4e, %xmm1, %xmm8
	mulpd	%xmm14, %xmm1
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm1
	movlps	%xmm1,   0 * SIZE(XX)
	movhps	%xmm1,   1 * SIZE(XX)
	addq	 INCX, XX

	movsd	 0 * SIZE(X), %xmm2
	movhps	 1 * SIZE(X), %xmm2
	addq	 INCX, X

	pshufd	$0x4e, %xmm2, %xmm8
	mulpd	%xmm14, %xmm2
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm2
	movlps	%xmm2,   0 * SIZE(XX)
	movhps	%xmm2,   1 * SIZE(XX)
	addq	 INCX, XX

	movsd	 0 * SIZE(X), %xmm3
	movhps	 1 * SIZE(X), %xmm3
	addq	 INCX, X

	pshufd	$0x4e, %xmm3, %xmm8
	mulpd	%xmm14, %xmm3
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm3
	movlps	%xmm3,   0 * SIZE(XX)
	movhps	%xmm3,   1 * SIZE(XX)
	addq	 INCX, XX
	ALIGN_3

.L226:
	testq	$2, M
	je	.L227

	movsd	 0 * SIZE(X), %xmm0
	movhps	 1 * SIZE(X), %xmm0
	addq	 INCX, X

	pshufd	$0x4e, %xmm0, %xmm8
	mulpd	%xmm14, %xmm0
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm0
	movlps	%xmm0,   0 * SIZE(XX)
	movhps	%xmm0,   1 * SIZE(XX)
	addq	 INCX, XX

	movsd	 0 * SIZE(X), %xmm1
	movhps	 1 * SIZE(X), %xmm1
	addq	 INCX, X

	pshufd	$0x4e, %xmm1, %xmm8
	mulpd	%xmm14, %xmm1
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm1
	movlps	%xmm1,   0 * SIZE(XX)
	movhps	%xmm1,   1 * SIZE(XX)
	addq	 INCX, XX
	ALIGN_3

.L227:
	testq	$1, M
	je	.L999

	movsd	 0 * SIZE(X), %xmm0
	movhps	 1 * SIZE(X), %xmm0

	pshufd	$0x4e, %xmm0, %xmm8
	mulpd	%xmm14, %xmm0
	mulpd	%xmm15, %xmm8
	addpd	%xmm8,  %xmm0

	movlps	%xmm0,  0 * SIZE(XX)
	movhps	%xmm0,  1 * SIZE(XX)
	ALIGN_3

.L999:
	xorq	%rax, %rax

	RESTOREREGISTERS

	ret

	EPILOGUE

