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
#define STACK_ALPHA_I	24 + STACK + ARGS(%esp)
#define STACK_X		32 + STACK + ARGS(%esp)
#define STACK_INCX	36 + STACK + ARGS(%esp)

#define M	%ebx
#define X	%ecx
#define INCX	%edx
#define I	%esi
#define XX	%edi
#define FLAG	%ebp

#include "l1param.h"

#if defined(NEHALEM) || defined(PENRYN) || defined(DUNNINGTON) || defined(SANDYBRIDGE)
#define  USE_PSHUFD
#else
#define USE_PSHUFD_HALF
#endif


#define xmm8	xmm0
#define xmm9	xmm1
#define xmm10	xmm2
#define xmm11	xmm3
#define xmm12	xmm4
#define xmm13	xmm5
#define xmm14	xmm6
#define xmm15	xmm7


	PROLOGUE
	PROFCODE

	pushl	%edi
	pushl	%esi
	pushl	%ebx
	pushl	%ebp

	movl	STACK_M,     M
	movl	STACK_X,     X
	movl	STACK_INCX,  INCX

	movsd	STACK_ALPHA_R, %xmm0
	movsd	STACK_ALPHA_I, %xmm1

	sall	$ZBASE_SHIFT, INCX
	xor	FLAG, FLAG

	testl	M, M
	jle	.L999

	xorps	%xmm7, %xmm7
	comisd	%xmm0, %xmm7
	jne	.L100

	comisd	%xmm1, %xmm7
	jne	.L100

/* Alpha == ZERO */
	cmpl	$2 * SIZE, INCX
	jne	.L20

/* INCX == 1 */
	testl	$SIZE, X
	je	.L05

	movsd	%xmm7, 0 * SIZE(X)
	addl	$SIZE, X
	movl	$1, FLAG
	decl	M
	jle	.L19
	ALIGN_3
.L05:

	movl	M,  I	# rcx = n
	sarl	$3, I
	jle	.L12
	ALIGN_4

.L11:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movaps	%xmm7,  0 * SIZE(X)
	movaps	%xmm7,  2 * SIZE(X)
	movaps	%xmm7,  4 * SIZE(X)
	movaps	%xmm7,  6 * SIZE(X)

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE + 64) - PREOFFSET(X)
#endif

	movaps	%xmm7,  8 * SIZE(X)
	movaps	%xmm7, 10 * SIZE(X)
	movaps	%xmm7, 12 * SIZE(X)
	movaps	%xmm7, 14 * SIZE(X)

	addl	$16 * SIZE, X
	decl	I
	jg	.L11
	ALIGN_4

.L12:
	testl	$4, M
	je	.L13

	movaps	%xmm7,  0 * SIZE(X)
	movaps	%xmm7,  2 * SIZE(X)
	movaps	%xmm7,  4 * SIZE(X)
	movaps	%xmm7,  6 * SIZE(X)
	addl	$8 * SIZE, X
	ALIGN_3

.L13:
	testl  $2, M
	je    .L14

	movaps	%xmm7,  0 * SIZE(X)
	movaps	%xmm7,  2 * SIZE(X)
	addl	$4 * SIZE, X
	ALIGN_3

.L14:
	testl  $1, M
	je    .L19
	movaps	%xmm7,  0 * SIZE(X)
	addl	$2 * SIZE, X
	ALIGN_3

.L19:
	testl  $1, FLAG
	je    .L999

	movsd	%xmm7, 0 * SIZE(X)
	jmp	.L999
	ALIGN_4

/* incx != 1 */
.L20:
	testl	$SIZE, X
	jne	.L30

/* Aligned Mode */
	movl	M,  I		# rcx = n
	sarl	$2, I
	jle	.L22
	ALIGN_4

.L21:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movaps	%xmm7, (X)
	addl	INCX, X
	movaps	%xmm7, (X)
	addl	INCX, X
	movaps	%xmm7, (X)
	addl	INCX, X
	movaps	%xmm7, (X)
	addl	INCX, X
	decl	I
	jg	.L21
	ALIGN_4

.L22:
	testl	$3, M
	je	.L999

	testl	$2, M
	je	.L23

	movaps	%xmm7, (X)
	addl	INCX, X
	movaps	%xmm7, (X)
	addl	INCX, X
	ALIGN_3

.L23:
	testl	$1, M
	je	.L999

	movaps	%xmm7, (X)
	jmp	.L999
	ALIGN_4


/* Unaligned Mode */
.L30:
	movl	M,  I		# rcx = n
	sarl	$2, I
	jle	.L32
	ALIGN_4

.L31:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movlps	%xmm7, 0 * SIZE(X)
	movlps	%xmm7, 1 * SIZE(X)
	addl	INCX, X
	movlps	%xmm7, 0 * SIZE(X)
	movlps	%xmm7, 1 * SIZE(X)
	addl	INCX, X
	movlps	%xmm7, 0 * SIZE(X)
	movlps	%xmm7, 1 * SIZE(X)
	addl	INCX, X
	movlps	%xmm7, 0 * SIZE(X)
	movlps	%xmm7, 1 * SIZE(X)
	addl	INCX, X
	decl	I
	jg	.L31
	ALIGN_4

.L32:
	testl	$3, M
	je	.L999

	testl	$2, M
	je	.L33

	movlps	%xmm7, 0 * SIZE(X)
	movlps	%xmm7, 1 * SIZE(X)
	addl	INCX, X
	movlps	%xmm7, 0 * SIZE(X)
	movlps	%xmm7, 1 * SIZE(X)
	addl	INCX, X
	ALIGN_3

.L33:
	testl	$1, M
	je	.L999

	movlps	%xmm7, 0 * SIZE(X)
	movlps	%xmm7, 1 * SIZE(X)
	jmp	.L999
	ALIGN_4

/* Alpha != ZERO */
.L100:
	testl	$SIZE, X
	jne	.L200

#ifdef HAVE_SSE3
	movddup	%xmm0, %xmm6
#else
	pshufd	$0x44, %xmm0, %xmm6
#endif

	xorps	%xmm7, %xmm7
	subsd	%xmm1, %xmm7
	movlhps	%xmm1, %xmm7

	cmpl	$2 * SIZE, INCX
	jne	.L120

	subl	$-16 * SIZE, X

	movl	M,  I
	sarl	$3, I
	jle	.L115

	movaps	-16 * SIZE(X), %xmm0
	movaps	-14 * SIZE(X), %xmm1
	movaps	-12 * SIZE(X), %xmm2
	movaps	-10 * SIZE(X), %xmm3

	decl	  I
	jle	  .L112
	ALIGN_4

.L111:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

#if defined(USE_PSHUFD) || defined(USE_PSHUFD_HALF)
	pshufd	$0x4e, %xmm0, %xmm5
#else
	movsd	 -15 * SIZE(X), %xmm5
	movhps	 -16 * SIZE(X), %xmm5
#endif
	mulpd	%xmm6, %xmm0
	mulpd	%xmm7, %xmm5
	addpd	%xmm5,  %xmm0
	movaps	%xmm0,  -16 * SIZE(X)
	movaps	 -8 * SIZE(X), %xmm0

#ifdef USE_PSHUFD
	pshufd	$0x4e, %xmm1, %xmm5
#else
	movsd	 -13 * SIZE(X), %xmm5
	movhps	 -14 * SIZE(X), %xmm5
#endif
	mulpd	%xmm6, %xmm1
	mulpd	%xmm7, %xmm5
	addpd	%xmm5,  %xmm1
	movaps	%xmm1,  -14 * SIZE(X)
	movaps	 -6 * SIZE(X), %xmm1

#if defined(USE_PSHUFD) || defined(USE_PSHUFD_HALF)
	pshufd	$0x4e, %xmm2, %xmm5
#else
	movsd	 -11 * SIZE(X), %xmm5
	movhps	 -12 * SIZE(X), %xmm5
#endif
	mulpd	%xmm6, %xmm2
	mulpd	%xmm7, %xmm5
	addpd	%xmm5,  %xmm2
	movaps	%xmm2,  -12 * SIZE(X)
	movaps	 -4 * SIZE(X), %xmm2

#ifdef USE_PSHUFD
	pshufd	$0x4e, %xmm3, %xmm5
#else
	movsd	  -9 * SIZE(X), %xmm5
	movhps	 -10 * SIZE(X), %xmm5
#endif
	mulpd	%xmm6, %xmm3
	mulpd	%xmm7, %xmm5
	addpd	%xmm5,  %xmm3
	movaps	%xmm3,  -10 * SIZE(X)
	movaps	 -2 * SIZE(X), %xmm3

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE + 64) - PREOFFSET(X)
#endif

#if defined(USE_PSHUFD) || defined(USE_PSHUFD_HALF)
	pshufd	$0x4e, %xmm0, %xmm5
#else
	movsd	  -7 * SIZE(X), %xmm5
	movhps	  -8 * SIZE(X), %xmm5
#endif
	mulpd	%xmm6, %xmm0
	mulpd	%xmm7, %xmm5
	addpd	%xmm5,  %xmm0
	movaps	%xmm0,   -8 * SIZE(X)
	movaps	  0 * SIZE(X), %xmm0

#ifdef USE_PSHUFD
	pshufd	$0x4e, %xmm1, %xmm5
#else
	movsd	  -5 * SIZE(X), %xmm5
	movhps	  -6 * SIZE(X), %xmm5
#endif
	mulpd	%xmm6, %xmm1
	mulpd	%xmm7, %xmm5
	addpd	%xmm5,  %xmm1
	movaps	%xmm1,   -6 * SIZE(X)
	movaps	  2 * SIZE(X), %xmm1

#if defined(USE_PSHUFD) || defined(USE_PSHUFD_HALF)
	pshufd	$0x4e, %xmm2, %xmm5
#else
	movsd	  -3 * SIZE(X), %xmm5
	movhps	  -4 * SIZE(X), %xmm5
#endif
	mulpd	%xmm6, %xmm2
	mulpd	%xmm7, %xmm5
	addpd	%xmm5,  %xmm2
	movaps	%xmm2,   -4 * SIZE(X)
	movaps	  4 * SIZE(X), %xmm2

#ifdef USE_PSHUFD
	pshufd	$0x4e, %xmm3, %xmm5
#else
	movsd	  -1 * SIZE(X), %xmm5
	movhps	  -2 * SIZE(X), %xmm5
#endif
	mulpd	%xmm6, %xmm3
	mulpd	%xmm7, %xmm5
	addpd	%xmm5,  %xmm3
	movaps	%xmm3,   -2 * SIZE(X)
	movaps	  6 * SIZE(X), %xmm3

	subl	$-16 * SIZE, X
	decl	I
	jg	.L111
	ALIGN_4

.L112:
#if defined(USE_PSHUFD) || defined(USE_PSHUFD_HALF)
	pshufd	$0x4e, %xmm0, %xmm5
#else
	movsd	 -15 * SIZE(X), %xmm5
	movhps	 -16 * SIZE(X), %xmm5
#endif
	mulpd	%xmm6, %xmm0
	mulpd	%xmm7, %xmm5
	addpd	%xmm5,  %xmm0
	movaps	%xmm0,  -16 * SIZE(X)
	movaps	 -8 * SIZE(X), %xmm0

#ifdef USE_PSHUFD
	pshufd	$0x4e, %xmm1, %xmm5
#else
	movsd	 -13 * SIZE(X), %xmm5
	movhps	 -14 * SIZE(X), %xmm5
#endif
	mulpd	%xmm6, %xmm1
	mulpd	%xmm7, %xmm5
	addpd	%xmm5,  %xmm1
	movaps	%xmm1,  -14 * SIZE(X)
	movaps	 -6 * SIZE(X), %xmm1

#if defined(USE_PSHUFD) || defined(USE_PSHUFD_HALF)
	pshufd	$0x4e, %xmm2, %xmm5
#else
	movsd	 -11 * SIZE(X), %xmm5
	movhps	 -12 * SIZE(X), %xmm5
#endif
	mulpd	%xmm6, %xmm2
	mulpd	%xmm7, %xmm5
	addpd	%xmm5,  %xmm2
	movaps	%xmm2,  -12 * SIZE(X)
	movaps	 -4 * SIZE(X), %xmm2

#ifdef USE_PSHUFD
	pshufd	$0x4e, %xmm3, %xmm5
#else
	movsd	  -9 * SIZE(X), %xmm5
	movhps	 -10 * SIZE(X), %xmm5
#endif
	mulpd	%xmm6, %xmm3
	mulpd	%xmm7, %xmm5
	addpd	%xmm5,  %xmm3
	movaps	%xmm3,  -10 * SIZE(X)
	movaps	 -2 * SIZE(X), %xmm3

#if defined(USE_PSHUFD) || defined(USE_PSHUFD_HALF)
	pshufd	$0x4e, %xmm0, %xmm5
#else
	movsd	  -7 * SIZE(X), %xmm5
	movhps	  -8 * SIZE(X), %xmm5
#endif
	mulpd	%xmm6, %xmm0
	mulpd	%xmm7, %xmm5
	addpd	%xmm5,  %xmm0
	movaps	%xmm0,   -8 * SIZE(X)

#ifdef USE_PSHUFD
	pshufd	$0x4e, %xmm1, %xmm5
#else
	movsd	  -5 * SIZE(X), %xmm5
	movhps	  -6 * SIZE(X), %xmm5
#endif
	mulpd	%xmm6, %xmm1
	mulpd	%xmm7, %xmm5
	addpd	%xmm5,  %xmm1
	movaps	%xmm1,   -6 * SIZE(X)

#if defined(USE_PSHUFD) || defined(USE_PSHUFD_HALF)
	pshufd	$0x4e, %xmm2, %xmm5
#else
	movsd	  -3 * SIZE(X), %xmm5
	movhps	  -4 * SIZE(X), %xmm5
#endif
	mulpd	%xmm6, %xmm2
	mulpd	%xmm7, %xmm5
	addpd	%xmm5,  %xmm2
	movaps	%xmm2,   -4 * SIZE(X)

#ifdef USE_PSHUFD
	pshufd	$0x4e, %xmm3, %xmm5
#else
	movsd	  -1 * SIZE(X), %xmm5
	movhps	  -2 * SIZE(X), %xmm5
#endif
	mulpd	%xmm6, %xmm3
	mulpd	%xmm7, %xmm5
	addpd	%xmm5,  %xmm3
	movaps	%xmm3,   -2 * SIZE(X)

	subl	$-16 * SIZE, X
	ALIGN_3

.L115:
	testl	$7, M
	je	.L999

	testl	$4, M
	je	.L116

	movaps	-16 * SIZE(X), %xmm0
	movaps	-14 * SIZE(X), %xmm1

	pshufd	$0x4e, %xmm0, %xmm5
	mulpd	%xmm6, %xmm0
	mulpd	%xmm7, %xmm5
	addpd	%xmm5, %xmm0
	movaps	%xmm0,  -16 * SIZE(X)

	pshufd	$0x4e, %xmm1, %xmm5
	mulpd	%xmm6, %xmm1
	mulpd	%xmm7, %xmm5
	addpd	%xmm5, %xmm1
	movaps	%xmm1,  -14 * SIZE(X)

	movaps	-12 * SIZE(X), %xmm2
	movaps	-10 * SIZE(X), %xmm3

	pshufd	$0x4e, %xmm2, %xmm5
	mulpd	%xmm6, %xmm2
	mulpd	%xmm7, %xmm5
	addpd	%xmm5, %xmm2
	movaps	%xmm2,  -12 * SIZE(X)

	pshufd	$0x4e, %xmm3, %xmm5
	mulpd	%xmm6, %xmm3
	mulpd	%xmm7, %xmm5
	addpd	%xmm5, %xmm3
	movaps	%xmm3,  -10 * SIZE(X)

	addl	$8 * SIZE, X
	ALIGN_3

.L116:
	testl	$2, M
	je	.L117

	movaps	-16 * SIZE(X), %xmm0
	movaps	-14 * SIZE(X), %xmm1

	pshufd	$0x4e, %xmm0, %xmm5
	mulpd	%xmm6, %xmm0
	mulpd	%xmm7, %xmm5
	addpd	%xmm5,  %xmm0
	movaps	%xmm0,  -16 * SIZE(X)

	pshufd	$0x4e, %xmm1, %xmm5
	mulpd	%xmm6, %xmm1
	mulpd	%xmm7, %xmm5
	addpd	%xmm5,  %xmm1
	movaps	%xmm1,  -14 * SIZE(X)

	addl	$4 * SIZE, X
	ALIGN_3

.L117:
	testl	$1, M
	je	.L999

	movaps	-16 * SIZE(X), %xmm0

	pshufd	$0x4e, %xmm0, %xmm5
	mulpd	%xmm6, %xmm0
	mulpd	%xmm7, %xmm5
	addpd	%xmm5, %xmm0

	movaps	%xmm0,  -16 * SIZE(X)
	jmp	.L999
	ALIGN_3

.L120:
	movl	X, XX

	movl	M,  I
	sarl	$3, I
	jle	.L125

	movaps	(X), %xmm0
	addl	INCX, X
	movaps	(X), %xmm1
	addl	INCX, X
	movaps	(X), %xmm2
	addl	INCX, X
	movaps	(X), %xmm3
	addl	INCX, X

	decl	  I
	jle	  .L122
	ALIGN_4

.L121:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	pshufd	$0x4e, %xmm0, %xmm5
	mulpd	%xmm6, %xmm0
	mulpd	%xmm7, %xmm5
	addpd	%xmm5, %xmm0
	movaps	%xmm0,  (XX)
	addl	INCX, XX
	movaps	(X), %xmm0
	addl	INCX, X

	pshufd	$0x4e, %xmm1, %xmm5
	mulpd	%xmm6, %xmm1
	mulpd	%xmm7, %xmm5
	addpd	%xmm5, %xmm1
	movaps	%xmm1,  (XX)
	addl	INCX, XX
	movaps	(X), %xmm1
	addl	INCX, X

	pshufd	$0x4e, %xmm2, %xmm5
	mulpd	%xmm6, %xmm2
	mulpd	%xmm7, %xmm5
	addpd	%xmm5, %xmm2
	movaps	%xmm2,  (XX)
	addl	INCX, XX
	movaps	(X), %xmm2
	addl	INCX, X

	pshufd	$0x4e, %xmm3, %xmm5
	mulpd	%xmm6, %xmm3
	mulpd	%xmm7, %xmm5
	addpd	%xmm5, %xmm3
	movaps	%xmm3,  (XX)
	addl	INCX, XX
	movaps	(X), %xmm3
	addl	INCX, X

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE + 64) - PREOFFSET(X)
#endif

	pshufd	$0x4e, %xmm0, %xmm5
	mulpd	%xmm6, %xmm0
	mulpd	%xmm7, %xmm5
	addpd	%xmm5, %xmm0
	movaps	%xmm0,  (XX)
	addl	INCX, XX
	movaps	(X), %xmm0
	addl	INCX, X

	pshufd	$0x4e, %xmm1, %xmm5
	mulpd	%xmm6, %xmm1
	mulpd	%xmm7, %xmm5
	addpd	%xmm5, %xmm1
	movaps	%xmm1,  (XX)
	addl	INCX, XX
	movaps	(X), %xmm1
	addl	INCX, X

	pshufd	$0x4e, %xmm2, %xmm5
	mulpd	%xmm6, %xmm2
	mulpd	%xmm7, %xmm5
	addpd	%xmm5, %xmm2
	movaps	%xmm2,  (XX)
	addl	INCX, XX
	movaps	(X), %xmm2
	addl	INCX, X

	pshufd	$0x4e, %xmm3, %xmm5
	mulpd	%xmm6, %xmm3
	mulpd	%xmm7, %xmm5
	addpd	%xmm5, %xmm3
	movaps	%xmm3,  (XX)
	addl	INCX, XX
	movaps	(X), %xmm3
	addl	INCX, X

	decl	I
	jg	.L121
	ALIGN_4

.L122:
	pshufd	$0x4e, %xmm0, %xmm5
	mulpd	%xmm6, %xmm0
	mulpd	%xmm7, %xmm5
	addpd	%xmm5, %xmm0
	movaps	%xmm0,  (XX)
	addl	INCX, XX
	movaps	(X), %xmm0
	addl	INCX, X

	pshufd	$0x4e, %xmm1, %xmm5
	mulpd	%xmm6, %xmm1
	mulpd	%xmm7, %xmm5
	addpd	%xmm5, %xmm1
	movaps	%xmm1,  (XX)
	addl	INCX, XX
	movaps	(X), %xmm1
	addl	INCX, X

	pshufd	$0x4e, %xmm2, %xmm5
	mulpd	%xmm6, %xmm2
	mulpd	%xmm7, %xmm5
	addpd	%xmm5, %xmm2
	movaps	%xmm2,  (XX)
	addl	INCX, XX
	movaps	(X), %xmm2
	addl	INCX, X

	pshufd	$0x4e, %xmm3, %xmm5
	mulpd	%xmm6, %xmm3
	mulpd	%xmm7, %xmm5
	addpd	%xmm5, %xmm3
	movaps	%xmm3,  (XX)
	addl	INCX, XX
	movaps	(X), %xmm3
	addl	INCX, X

	pshufd	$0x4e, %xmm0, %xmm5
	mulpd	%xmm6, %xmm0
	mulpd	%xmm7, %xmm5
	addpd	%xmm5, %xmm0
	movaps	%xmm0,  (XX)
	addl	INCX, XX

	pshufd	$0x4e, %xmm1, %xmm5
	mulpd	%xmm6, %xmm1
	mulpd	%xmm7, %xmm5
	addpd	%xmm5, %xmm1
	movaps	%xmm1,  (XX)
	addl	INCX, XX

	pshufd	$0x4e, %xmm2, %xmm5
	mulpd	%xmm6, %xmm2
	mulpd	%xmm7, %xmm5
	addpd	%xmm5, %xmm2
	movaps	%xmm2,  (XX)
	addl	INCX, XX

	pshufd	$0x4e, %xmm3, %xmm5
	mulpd	%xmm6, %xmm3
	mulpd	%xmm7, %xmm5
	addpd	%xmm5, %xmm3
	movaps	%xmm3,  (XX)
	addl	INCX, XX
	ALIGN_3

.L125:
	testl	$7, M
	je	.L999

	testl	$4, M
	je	.L126

	movaps	(X), %xmm0
	addl	INCX, X
	movaps	(X), %xmm1
	addl	INCX, X

	movaps	(X), %xmm2
	addl	INCX, X
	movaps	(X), %xmm3
	addl	INCX, X

	pshufd	$0x4e, %xmm0, %xmm5
	mulpd	%xmm6, %xmm0
	mulpd	%xmm7, %xmm5
	addpd	%xmm5, %xmm0
	movaps	%xmm0,  (XX)
	addl	INCX, XX

	pshufd	$0x4e, %xmm1, %xmm5
	mulpd	%xmm6, %xmm1
	mulpd	%xmm7, %xmm5
	addpd	%xmm5, %xmm1
	movaps	%xmm1,  (XX)
	addl	INCX, XX

	pshufd	$0x4e, %xmm2, %xmm5
	mulpd	%xmm6, %xmm2
	mulpd	%xmm7, %xmm5
	addpd	%xmm5, %xmm2
	movaps	%xmm2,  (XX)
	addl	INCX, XX

	pshufd	$0x4e, %xmm3, %xmm5
	mulpd	%xmm6, %xmm3
	mulpd	%xmm7, %xmm5
	addpd	%xmm5, %xmm3
	movaps	%xmm3,  (XX)
	addl	INCX, XX
	ALIGN_3

.L126:
	testl	$2, M
	je	.L127

	movaps	(X), %xmm0
	addl	INCX, X
	movaps	(X), %xmm1
	addl	INCX, X

	pshufd	$0x4e, %xmm0, %xmm5
	mulpd	%xmm6, %xmm0
	mulpd	%xmm7, %xmm5
	addpd	%xmm5, %xmm0
	movaps	%xmm0,  (XX)
	addl	INCX, XX

	pshufd	$0x4e, %xmm1, %xmm5
	mulpd	%xmm6, %xmm1
	mulpd	%xmm7, %xmm5
	addpd	%xmm5, %xmm1
	movaps	%xmm1,  (XX)
	addl	INCX, XX
	ALIGN_3

.L127:
	testl	$1, M
	je	.L999

	movaps	(X), %xmm0

	pshufd	$0x4e, %xmm0, %xmm5
	mulpd	%xmm6, %xmm0
	mulpd	%xmm7, %xmm5
	addpd	%xmm5, %xmm0

	movaps	%xmm0,  (XX)
	jmp	.L999
	ALIGN_3

.L200:
	cmpl	$2 * SIZE, INCX
	jne	.L220

#if defined(ALIGNED_ACCESS) && !defined(NEHALEM) && !defined(SANDYBRIDGE)

#ifdef HAVE_SSE3
	movddup	%xmm0, %xmm6
#else
	pshufd	$0x44, %xmm0, %xmm6
#endif
	pxor	%xmm7,  %xmm7
	subsd	%xmm1,  %xmm7
	movlhps	%xmm1,  %xmm7
	shufpd	$1, %xmm7, %xmm7

	movhps	0 * SIZE(X), %xmm0
	movaps	1 * SIZE(X), %xmm1
	subl	$-16 * SIZE, X

	unpckhpd %xmm0, %xmm0
	mulsd	%xmm6, %xmm0
	movaps	%xmm1, %xmm5
	mulsd	%xmm7, %xmm5
	subsd	%xmm5, %xmm0
	movlps	%xmm0, -16 * SIZE(X)

	decl	M

	movl	M,  I
	sarl	$3, I
	jle	.L205

	movaps	-13 * SIZE(X), %xmm2
	movaps	-11 * SIZE(X), %xmm3

	decl	  I
	jle	  .L202
	ALIGN_4

.L201:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	movaps	%xmm1, %xmm5
	SHUFPD_1 %xmm2, %xmm0
	mulpd	%xmm6, %xmm5
	mulpd	%xmm7, %xmm0
	addpd	%xmm5, %xmm0
	movaps	%xmm0,  -15 * SIZE(X)
	movaps	 -9 * SIZE(X), %xmm0

	movaps	%xmm2, %xmm5
	SHUFPD_1 %xmm3, %xmm1
	mulpd	%xmm6, %xmm5
	mulpd	%xmm7, %xmm1
	addpd	%xmm5, %xmm1
	movaps	%xmm1,  -13 * SIZE(X)
	movaps	 -7 * SIZE(X), %xmm1

	movaps	%xmm3, %xmm5
	SHUFPD_1 %xmm0, %xmm2
	mulpd	%xmm6, %xmm5
	mulpd	%xmm7, %xmm2
	addpd	%xmm5, %xmm2
	movaps	%xmm2,  -11 * SIZE(X)
	movaps	 -5 * SIZE(X), %xmm2

	movaps	%xmm0, %xmm5
	SHUFPD_1 %xmm1, %xmm3
	mulpd	%xmm6, %xmm5
	mulpd	%xmm7, %xmm3
	addpd	%xmm5, %xmm3
	movaps	%xmm3,   -9 * SIZE(X)
	movaps	 -3 * SIZE(X), %xmm3

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE + 64) - PREOFFSET(X)
#endif

	movaps	%xmm1, %xmm5
	SHUFPD_1 %xmm2, %xmm0
	mulpd	%xmm6, %xmm5
	mulpd	%xmm7, %xmm0
	addpd	%xmm5, %xmm0
	movaps	%xmm0,   -7 * SIZE(X)
	movaps	 -1 * SIZE(X), %xmm0

	movaps	%xmm2, %xmm5
	SHUFPD_1 %xmm3, %xmm1
	mulpd	%xmm6, %xmm5
	mulpd	%xmm7, %xmm1
	addpd	%xmm5, %xmm1
	movaps	%xmm1,   -5 * SIZE(X)
	movaps	  1 * SIZE(X), %xmm1

	movaps	%xmm3, %xmm5
	SHUFPD_1 %xmm0, %xmm2
	mulpd	%xmm6, %xmm5
	mulpd	%xmm7, %xmm2
	addpd	%xmm5, %xmm2
	movaps	%xmm2,   -3 * SIZE(X)
	movaps	  3 * SIZE(X), %xmm2

	movaps	%xmm0, %xmm5
	SHUFPD_1 %xmm1, %xmm3
	mulpd	%xmm6, %xmm5
	mulpd	%xmm7, %xmm3
	addpd	%xmm5, %xmm3
	movaps	%xmm3,   -1 * SIZE(X)
	movaps	  5 * SIZE(X), %xmm3

	subl	$-16 * SIZE, X
	decl	I
	jg	.L201
	ALIGN_4

.L202:
	movaps	%xmm1, %xmm5
	SHUFPD_1 %xmm2, %xmm0
	mulpd	%xmm6, %xmm5
	mulpd	%xmm7, %xmm0
	addpd	%xmm5, %xmm0
	movaps	%xmm0,  -15 * SIZE(X)
	movaps	 -9 * SIZE(X), %xmm0

	movaps	%xmm2, %xmm5
	SHUFPD_1 %xmm3, %xmm1
	mulpd	%xmm6, %xmm5
	mulpd	%xmm7, %xmm1
	addpd	%xmm5, %xmm1
	movaps	%xmm1,  -13 * SIZE(X)
	movaps	 -7 * SIZE(X), %xmm1

	movaps	%xmm3, %xmm5
	SHUFPD_1 %xmm0, %xmm2
	mulpd	%xmm6, %xmm5
	mulpd	%xmm7, %xmm2
	addpd	%xmm5, %xmm2
	movaps	%xmm2,  -11 * SIZE(X)
	movaps	 -5 * SIZE(X), %xmm2

	movaps	%xmm0, %xmm5
	SHUFPD_1 %xmm1, %xmm3
	mulpd	%xmm6, %xmm5
	mulpd	%xmm7, %xmm3
	addpd	%xmm5, %xmm3
	movaps	%xmm3,   -9 * SIZE(X)
	movaps	 -3 * SIZE(X), %xmm3

	movaps	%xmm1, %xmm5
	SHUFPD_1 %xmm2, %xmm0
	mulpd	%xmm6, %xmm5
	mulpd	%xmm7, %xmm0
	addpd	%xmm5, %xmm0
	movaps	%xmm0,   -7 * SIZE(X)
	movaps	 -1 * SIZE(X), %xmm0

	movaps	%xmm2, %xmm5
	SHUFPD_1 %xmm3, %xmm1
	mulpd	%xmm6, %xmm5
	mulpd	%xmm7, %xmm1
	addpd	%xmm5, %xmm1
	movaps	%xmm1,   -5 * SIZE(X)
	movaps	  1 * SIZE(X), %xmm1

	movaps	%xmm3, %xmm5
	SHUFPD_1 %xmm0, %xmm2
	mulpd	%xmm6, %xmm5
	mulpd	%xmm7, %xmm2
	addpd	%xmm5, %xmm2
	movaps	%xmm2,   -3 * SIZE(X)

	movaps	%xmm0, %xmm5
	SHUFPD_1 %xmm1, %xmm3
	mulpd	%xmm6, %xmm5
	mulpd	%xmm7, %xmm3
	addpd	%xmm5, %xmm3
	movaps	%xmm3,   -1 * SIZE(X)

	subl	$-16 * SIZE, X
	ALIGN_3

.L205:
	testl	$4, M
	je	.L206

	movaps	-13 * SIZE(X), %xmm2

	movaps	%xmm1, %xmm5
	SHUFPD_1 %xmm2, %xmm0
	mulpd	%xmm6, %xmm5
	mulpd	%xmm7, %xmm0
	addpd	%xmm5, %xmm0
	movaps	%xmm0,  -15 * SIZE(X)

	movaps	-11 * SIZE(X), %xmm3

	movaps	%xmm2, %xmm5
	SHUFPD_1 %xmm3, %xmm1
	mulpd	%xmm6, %xmm5
	mulpd	%xmm7, %xmm1
	addpd	%xmm5, %xmm1
	movaps	%xmm1,  -13 * SIZE(X)

	movaps	 -9 * SIZE(X), %xmm0

	movaps	%xmm3, %xmm5
	SHUFPD_1 %xmm0, %xmm2
	mulpd	%xmm6, %xmm5
	mulpd	%xmm7, %xmm2
	addpd	%xmm5, %xmm2
	movaps	%xmm2,  -11 * SIZE(X)

	movaps	 -7 * SIZE(X), %xmm1

	movaps	%xmm0, %xmm5
	SHUFPD_1 %xmm1, %xmm3
	mulpd	%xmm6, %xmm5
	mulpd	%xmm7, %xmm3
	addpd	%xmm5, %xmm3
	movaps	%xmm3,   -9 * SIZE(X)

	addl	$8 * SIZE, X
	ALIGN_3

.L206:
	testl	$2, M
	je	.L207

	movaps	-13 * SIZE(X), %xmm2

	movaps	%xmm1, %xmm5
	SHUFPD_1 %xmm2, %xmm0

	mulpd	%xmm6, %xmm5
	mulpd	%xmm7, %xmm0
	addpd	%xmm5, %xmm0
	movaps	%xmm0,  -15 * SIZE(X)

	movaps	-11 * SIZE(X), %xmm3

	movaps	%xmm2, %xmm5
	SHUFPD_1 %xmm3, %xmm1

	mulpd	%xmm6, %xmm5
	mulpd	%xmm7, %xmm1
	addpd	%xmm5, %xmm1
	movaps	%xmm1,  -13 * SIZE(X)

	movaps	%xmm2, %xmm0
	movaps	%xmm3, %xmm1

	addl	$4 * SIZE, X
	ALIGN_3

.L207:
	testl	$1, M
	je	.L208

	movaps	-13 * SIZE(X), %xmm2

	movaps	%xmm1,  %xmm5
	SHUFPD_1 %xmm2, %xmm0

	mulpd	%xmm6, %xmm5
	mulpd	%xmm7, %xmm0
	addpd	%xmm5,  %xmm0
	movaps	%xmm0,  -15 * SIZE(X)

	movaps	%xmm1, %xmm0
	movaps	%xmm2, %xmm1
	addl	$2 * SIZE, X
	ALIGN_3

.L208:
	unpckhpd %xmm0, %xmm0
	mulsd	%xmm6, %xmm1
	mulsd	%xmm7, %xmm0
	addsd	%xmm1, %xmm0
	movlps	%xmm0, -15 * SIZE(X)
	jmp	.L999
	ALIGN_3

#else

#ifdef HAVE_SSE3
	movddup	%xmm0, %xmm6
#else
	pshufd	$0x44, %xmm0, %xmm6
#endif
	pxor	%xmm7, %xmm7
	subsd	%xmm1,  %xmm7
	movlhps	%xmm1,  %xmm7

	subl	$-16 * SIZE, X

	movl	M,  I
	sarl	$3, I
	jle	.L205

	movsd	-16 * SIZE(X), %xmm0
	movhps	-15 * SIZE(X), %xmm0
	movsd	-14 * SIZE(X), %xmm1
	movhps	-13 * SIZE(X), %xmm1
	movsd	-12 * SIZE(X), %xmm2
	movhps	-11 * SIZE(X), %xmm2
	movsd	-10 * SIZE(X), %xmm3
	movhps	 -9 * SIZE(X), %xmm3

	decl	  I
	jle	  .L202
	ALIGN_4

.L201:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	pshufd	$0x4e, %xmm0, %xmm5
	mulpd	%xmm6, %xmm0
	mulpd	%xmm7, %xmm5
	addpd	%xmm5,  %xmm0
	movlps	%xmm0,  -16 * SIZE(X)
	movhps	%xmm0,  -15 * SIZE(X)
	movsd	 -8 * SIZE(X), %xmm0
	movhps	 -7 * SIZE(X), %xmm0

	pshufd	$0x4e, %xmm1, %xmm5
	mulpd	%xmm6, %xmm1
	mulpd	%xmm7, %xmm5
	addpd	%xmm5,  %xmm1
	movlps	%xmm1,  -14 * SIZE(X)
	movhps	%xmm1,  -13 * SIZE(X)
	movsd	 -6 * SIZE(X), %xmm1
	movhps	 -5 * SIZE(X), %xmm1

	pshufd	$0x4e, %xmm2, %xmm5
	mulpd	%xmm6, %xmm2
	mulpd	%xmm7, %xmm5
	addpd	%xmm5,  %xmm2
	movlps	%xmm2,  -12 * SIZE(X)
	movhps	%xmm2,  -11 * SIZE(X)
	movsd	 -4 * SIZE(X), %xmm2
	movhps	 -3 * SIZE(X), %xmm2

	pshufd	$0x4e, %xmm3, %xmm5
	mulpd	%xmm6, %xmm3
	mulpd	%xmm7, %xmm5
	addpd	%xmm5,  %xmm3
	movlps	%xmm3,  -10 * SIZE(X)
	movhps	%xmm3,   -9 * SIZE(X)
	movsd	 -2 * SIZE(X), %xmm3
	movhps	 -1 * SIZE(X), %xmm3

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE + 64) - PREOFFSET(X)
#endif

	pshufd	$0x4e, %xmm0, %xmm5
	mulpd	%xmm6, %xmm0
	mulpd	%xmm7, %xmm5
	addpd	%xmm5,  %xmm0
	movlps	%xmm0,   -8 * SIZE(X)
	movhps	%xmm0,   -7 * SIZE(X)
	movsd	  0 * SIZE(X), %xmm0
	movhps	  1 * SIZE(X), %xmm0

	pshufd	$0x4e, %xmm1, %xmm5
	mulpd	%xmm6, %xmm1
	mulpd	%xmm7, %xmm5
	addpd	%xmm5,  %xmm1
	movlps	%xmm1,   -6 * SIZE(X)
	movhps	%xmm1,   -5 * SIZE(X)
	movsd	  2 * SIZE(X), %xmm1
	movhps	  3 * SIZE(X), %xmm1

	pshufd	$0x4e, %xmm2, %xmm5
	mulpd	%xmm6, %xmm2
	mulpd	%xmm7, %xmm5
	addpd	%xmm5,  %xmm2
	movlps	%xmm2,   -4 * SIZE(X)
	movhps	%xmm2,   -3 * SIZE(X)
	movsd	  4 * SIZE(X), %xmm2
	movhps	  5 * SIZE(X), %xmm2

	pshufd	$0x4e, %xmm3, %xmm5
	mulpd	%xmm6, %xmm3
	mulpd	%xmm7, %xmm5
	addpd	%xmm5,  %xmm3
	movlps	%xmm3,   -2 * SIZE(X)
	movhps	%xmm3,   -1 * SIZE(X)
	movsd	  6 * SIZE(X), %xmm3
	movhps	  7 * SIZE(X), %xmm3

	subl	$-16 * SIZE, X
	decl	I
	jg	.L201
	ALIGN_4

.L202:
	pshufd	$0x4e, %xmm0, %xmm5
	mulpd	%xmm6, %xmm0
	mulpd	%xmm7, %xmm5
	addpd	%xmm5,  %xmm0
	movlps	%xmm0,  -16 * SIZE(X)
	movhps	%xmm0,  -15 * SIZE(X)
	movsd	 -8 * SIZE(X), %xmm0
	movhps	 -7 * SIZE(X), %xmm0

	pshufd	$0x4e, %xmm1, %xmm5
	mulpd	%xmm6, %xmm1
	mulpd	%xmm7, %xmm5
	addpd	%xmm5,  %xmm1
	movlps	%xmm1,  -14 * SIZE(X)
	movhps	%xmm1,  -13 * SIZE(X)
	movsd	 -6 * SIZE(X), %xmm1
	movhps	 -5 * SIZE(X), %xmm1

	pshufd	$0x4e, %xmm2, %xmm5
	mulpd	%xmm6, %xmm2
	mulpd	%xmm7, %xmm5
	addpd	%xmm5,  %xmm2
	movlps	%xmm2,  -12 * SIZE(X)
	movhps	%xmm2,  -11 * SIZE(X)
	movsd	 -4 * SIZE(X), %xmm2
	movhps	 -3 * SIZE(X), %xmm2

	pshufd	$0x4e, %xmm3, %xmm5
	mulpd	%xmm6, %xmm3
	mulpd	%xmm7, %xmm5
	addpd	%xmm5,  %xmm3
	movlps	%xmm3,  -10 * SIZE(X)
	movhps	%xmm3,   -9 * SIZE(X)
	movsd	 -2 * SIZE(X), %xmm3
	movhps	 -1 * SIZE(X), %xmm3

	pshufd	$0x4e, %xmm0, %xmm5
	mulpd	%xmm6, %xmm0
	mulpd	%xmm7, %xmm5
	addpd	%xmm5,  %xmm0
	movlps	%xmm0,   -8 * SIZE(X)
	movhps	%xmm0,   -7 * SIZE(X)

	pshufd	$0x4e, %xmm1, %xmm5
	mulpd	%xmm6, %xmm1
	mulpd	%xmm7, %xmm5
	addpd	%xmm5,  %xmm1
	movlps	%xmm1,   -6 * SIZE(X)
	movhps	%xmm1,   -5 * SIZE(X)

	pshufd	$0x4e, %xmm2, %xmm5
	mulpd	%xmm6, %xmm2
	mulpd	%xmm7, %xmm5
	addpd	%xmm5,  %xmm2
	movlps	%xmm2,   -4 * SIZE(X)
	movhps	%xmm2,   -3 * SIZE(X)

	pshufd	$0x4e, %xmm3, %xmm5
	mulpd	%xmm6, %xmm3
	mulpd	%xmm7, %xmm5
	addpd	%xmm5,  %xmm3
	movlps	%xmm3,   -2 * SIZE(X)
	movhps	%xmm3,   -1 * SIZE(X)

	subl	$-16 * SIZE, X
	ALIGN_3

.L205:
	testl	$7, M
	je	.L999

	testl	$4, M
	je	.L206

	movsd	-16 * SIZE(X), %xmm0
	movhps	-15 * SIZE(X), %xmm0
	movsd	-14 * SIZE(X), %xmm1
	movhps	-13 * SIZE(X), %xmm1

	pshufd	$0x4e, %xmm0, %xmm5
	mulpd	%xmm6, %xmm0
	mulpd	%xmm7, %xmm5
	addpd	%xmm5,  %xmm0
	movlps	%xmm0,  -16 * SIZE(X)
	movhps	%xmm0,  -15 * SIZE(X)

	pshufd	$0x4e, %xmm1, %xmm5
	mulpd	%xmm6, %xmm1
	mulpd	%xmm7, %xmm5
	addpd	%xmm5,  %xmm1
	movlps	%xmm1,  -14 * SIZE(X)
	movhps	%xmm1,  -13 * SIZE(X)

	movsd	-12 * SIZE(X), %xmm2
	movhps	-11 * SIZE(X), %xmm2
	movsd	-10 * SIZE(X), %xmm3
	movhps	 -9 * SIZE(X), %xmm3

	pshufd	$0x4e, %xmm2, %xmm5
	mulpd	%xmm6, %xmm2
	mulpd	%xmm7, %xmm5
	addpd	%xmm5,  %xmm2
	movlps	%xmm2,  -12 * SIZE(X)
	movhps	%xmm2,  -11 * SIZE(X)

	pshufd	$0x4e, %xmm3, %xmm5
	mulpd	%xmm6, %xmm3
	mulpd	%xmm7, %xmm5
	addpd	%xmm5,  %xmm3
	movlps	%xmm3,  -10 * SIZE(X)
	movhps	%xmm3,   -9 * SIZE(X)

	addl	$8 * SIZE, X
	ALIGN_3

.L206:
	testl	$2, M
	je	.L207

	movsd	-16 * SIZE(X), %xmm0
	movhps	-15 * SIZE(X), %xmm0

	pshufd	$0x4e, %xmm0, %xmm5
	mulpd	%xmm6, %xmm0
	mulpd	%xmm7, %xmm5
	addpd	%xmm5,  %xmm0
	movlps	%xmm0,  -16 * SIZE(X)
	movhps	%xmm0,  -15 * SIZE(X)

	movsd	-14 * SIZE(X), %xmm1
	movhps	-13 * SIZE(X), %xmm1

	pshufd	$0x4e, %xmm1, %xmm5
	mulpd	%xmm6, %xmm1
	mulpd	%xmm7, %xmm5
	addpd	%xmm5,  %xmm1
	movlps	%xmm1,  -14 * SIZE(X)
	movhps	%xmm1,  -13 * SIZE(X)

	addl	$4 * SIZE, X
	ALIGN_3

.L207:
	testl	$1, M
	je	.L999

	movsd	-16 * SIZE(X), %xmm0
	movhps	-15 * SIZE(X), %xmm0

	pshufd	$0x4e, %xmm0, %xmm5
	mulpd	%xmm6, %xmm0
	mulpd	%xmm7, %xmm5
	addpd	%xmm5, %xmm0

	movlps	%xmm0,  -16 * SIZE(X)
	movhps	%xmm0,  -15 * SIZE(X)
	jmp	.L999
	ALIGN_3

#endif

.L220:
#ifdef HAVE_SSE3
	movddup	%xmm0, %xmm6
#else
	pshufd	$0x44, %xmm0, %xmm6
#endif
	pxor	%xmm7, %xmm7
	subsd	%xmm1, %xmm7
	movlhps	%xmm1, %xmm7

	movl	X, XX

	movl	M,  I
	sarl	$3, I
	jle	.L225

	movsd	 0 * SIZE(X), %xmm0
	movhps	 1 * SIZE(X), %xmm0
	addl	 INCX, X
	movsd	 0 * SIZE(X), %xmm1
	movhps	 1 * SIZE(X), %xmm1
	addl	 INCX, X
	movsd	 0 * SIZE(X), %xmm2
	movhps	 1 * SIZE(X), %xmm2
	addl	 INCX, X
	movsd	 0 * SIZE(X), %xmm3
	movhps	 1 * SIZE(X), %xmm3
	addl	 INCX, X

	decl	  I
	jle	  .L222
	ALIGN_4

.L221:
#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE +  0) - PREOFFSET(X)
#endif

	pshufd	$0x4e, %xmm0, %xmm5
	mulpd	%xmm6, %xmm0
	mulpd	%xmm7, %xmm5
	addpd	%xmm5, %xmm0
	movlps	%xmm0,   0 * SIZE(XX)
	movhps	%xmm0,   1 * SIZE(XX)
	addl	 INCX, XX
	movsd	 0 * SIZE(X), %xmm0
	movhps	 1 * SIZE(X), %xmm0
	addl	 INCX, X

	pshufd	$0x4e, %xmm1, %xmm5
	mulpd	%xmm6, %xmm1
	mulpd	%xmm7, %xmm5
	addpd	%xmm5, %xmm1
	movlps	%xmm1,   0 * SIZE(XX)
	movhps	%xmm1,   1 * SIZE(XX)
	addl	 INCX, XX
	movsd	 0 * SIZE(X), %xmm1
	movhps	 1 * SIZE(X), %xmm1
	addl	 INCX, X

	pshufd	$0x4e, %xmm2, %xmm5
	mulpd	%xmm6, %xmm2
	mulpd	%xmm7, %xmm5
	addpd	%xmm5, %xmm2
	movlps	%xmm2,   0 * SIZE(XX)
	movhps	%xmm2,   1 * SIZE(XX)
	addl	 INCX, XX
	movsd	 0 * SIZE(X), %xmm2
	movhps	 1 * SIZE(X), %xmm2
	addl	 INCX, X

	pshufd	$0x4e, %xmm3, %xmm5
	mulpd	%xmm6, %xmm3
	mulpd	%xmm7, %xmm5
	addpd	%xmm5, %xmm3
	movlps	%xmm3,   0 * SIZE(XX)
	movhps	%xmm3,   1 * SIZE(XX)
	addl	 INCX, XX
	movsd	 0 * SIZE(X), %xmm3
	movhps	 1 * SIZE(X), %xmm3
	addl	 INCX, X

#ifdef PREFETCHW
	PREFETCHW (PREFETCHSIZE + 64) - PREOFFSET(X)
#endif

	pshufd	$0x4e, %xmm0, %xmm5
	mulpd	%xmm6, %xmm0
	mulpd	%xmm7, %xmm5
	addpd	%xmm5, %xmm0
	movlps	%xmm0,   0 * SIZE(XX)
	movhps	%xmm0,   1 * SIZE(XX)
	addl	 INCX, XX
	movsd	 0 * SIZE(X), %xmm0
	movhps	 1 * SIZE(X), %xmm0
	addl	 INCX, X

	pshufd	$0x4e, %xmm1, %xmm5
	mulpd	%xmm6, %xmm1
	mulpd	%xmm7, %xmm5
	addpd	%xmm5, %xmm1
	movlps	%xmm1,   0 * SIZE(XX)
	movhps	%xmm1,   1 * SIZE(XX)
	addl	 INCX, XX
	movsd	 0 * SIZE(X), %xmm1
	movhps	 1 * SIZE(X), %xmm1
	addl	 INCX, X

	pshufd	$0x4e, %xmm2, %xmm5
	mulpd	%xmm6, %xmm2
	mulpd	%xmm7, %xmm5
	addpd	%xmm5, %xmm2
	movlps	%xmm2,   0 * SIZE(XX)
	movhps	%xmm2,   1 * SIZE(XX)
	addl	 INCX, XX
	movsd	 0 * SIZE(X), %xmm2
	movhps	 1 * SIZE(X), %xmm2
	addl	 INCX, X

	pshufd	$0x4e, %xmm3, %xmm5
	mulpd	%xmm6, %xmm3
	mulpd	%xmm7, %xmm5
	addpd	%xmm5, %xmm3
	movlps	%xmm3,   0 * SIZE(XX)
	movhps	%xmm3,   1 * SIZE(XX)
	addl	 INCX, XX
	movsd	 0 * SIZE(X), %xmm3
	movhps	 1 * SIZE(X), %xmm3
	addl	 INCX, X

	decl	I
	jg	.L221
	ALIGN_4

.L222:
	pshufd	$0x4e, %xmm0, %xmm5
	mulpd	%xmm6, %xmm0
	mulpd	%xmm7, %xmm5
	addpd	%xmm5, %xmm0
	movlps	%xmm0,   0 * SIZE(XX)
	movhps	%xmm0,   1 * SIZE(XX)
	addl	 INCX, XX
	movsd	 0 * SIZE(X), %xmm0
	movhps	 1 * SIZE(X), %xmm0
	addl	 INCX, X

	pshufd	$0x4e, %xmm1, %xmm5
	mulpd	%xmm6, %xmm1
	mulpd	%xmm7, %xmm5
	addpd	%xmm5, %xmm1
	movlps	%xmm1,   0 * SIZE(XX)
	movhps	%xmm1,   1 * SIZE(XX)
	addl	 INCX, XX
	movsd	 0 * SIZE(X), %xmm1
	movhps	 1 * SIZE(X), %xmm1
	addl	 INCX, X

	pshufd	$0x4e, %xmm2, %xmm5
	mulpd	%xmm6, %xmm2
	mulpd	%xmm7, %xmm5
	addpd	%xmm5, %xmm2
	movlps	%xmm2,   0 * SIZE(XX)
	movhps	%xmm2,   1 * SIZE(XX)
	addl	 INCX, XX
	movsd	 0 * SIZE(X), %xmm2
	movhps	 1 * SIZE(X), %xmm2
	addl	 INCX, X

	pshufd	$0x4e, %xmm3, %xmm5
	mulpd	%xmm6, %xmm3
	mulpd	%xmm7, %xmm5
	addpd	%xmm5, %xmm3
	movlps	%xmm3,   0 * SIZE(XX)
	movhps	%xmm3,   1 * SIZE(XX)
	addl	 INCX, XX
	movsd	 0 * SIZE(X), %xmm3
	movhps	 1 * SIZE(X), %xmm3
	addl	 INCX, X

	pshufd	$0x4e, %xmm0, %xmm5
	mulpd	%xmm6, %xmm0
	mulpd	%xmm7, %xmm5
	addpd	%xmm5, %xmm0
	movlps	%xmm0,   0 * SIZE(XX)
	movhps	%xmm0,   1 * SIZE(XX)
	addl	 INCX, XX

	pshufd	$0x4e, %xmm1, %xmm5
	mulpd	%xmm6, %xmm1
	mulpd	%xmm7, %xmm5
	addpd	%xmm5, %xmm1
	movlps	%xmm1,   0 * SIZE(XX)
	movhps	%xmm1,   1 * SIZE(XX)
	addl	 INCX, XX

	pshufd	$0x4e, %xmm2, %xmm5
	mulpd	%xmm6, %xmm2
	mulpd	%xmm7, %xmm5
	addpd	%xmm5, %xmm2
	movlps	%xmm2,   0 * SIZE(XX)
	movhps	%xmm2,   1 * SIZE(XX)
	addl	 INCX, XX

	pshufd	$0x4e, %xmm3, %xmm5
	mulpd	%xmm6, %xmm3
	mulpd	%xmm7, %xmm5
	addpd	%xmm5, %xmm3
	movlps	%xmm3,   0 * SIZE(XX)
	movhps	%xmm3,   1 * SIZE(XX)
	addl	 INCX, XX
	ALIGN_3

.L225:
	testl	$7, M
	je	.L999

	testl	$4, M
	je	.L226

	movsd	 0 * SIZE(X), %xmm0
	movhps	 1 * SIZE(X), %xmm0
	addl	 INCX, X

	pshufd	$0x4e, %xmm0, %xmm5
	mulpd	%xmm6, %xmm0
	mulpd	%xmm7, %xmm5
	addpd	%xmm5, %xmm0
	movlps	%xmm0,   0 * SIZE(XX)
	movhps	%xmm0,   1 * SIZE(XX)
	addl	 INCX, XX

	movsd	 0 * SIZE(X), %xmm1
	movhps	 1 * SIZE(X), %xmm1
	addl	 INCX, X

	pshufd	$0x4e, %xmm1, %xmm5
	mulpd	%xmm6, %xmm1
	mulpd	%xmm7, %xmm5
	addpd	%xmm5, %xmm1
	movlps	%xmm1,   0 * SIZE(XX)
	movhps	%xmm1,   1 * SIZE(XX)
	addl	 INCX, XX

	movsd	 0 * SIZE(X), %xmm2
	movhps	 1 * SIZE(X), %xmm2
	addl	 INCX, X

	pshufd	$0x4e, %xmm2, %xmm5
	mulpd	%xmm6, %xmm2
	mulpd	%xmm7, %xmm5
	addpd	%xmm5, %xmm2
	movlps	%xmm2,   0 * SIZE(XX)
	movhps	%xmm2,   1 * SIZE(XX)
	addl	 INCX, XX

	movsd	 0 * SIZE(X), %xmm3
	movhps	 1 * SIZE(X), %xmm3
	addl	 INCX, X

	pshufd	$0x4e, %xmm3, %xmm5
	mulpd	%xmm6, %xmm3
	mulpd	%xmm7, %xmm5
	addpd	%xmm5, %xmm3
	movlps	%xmm3,   0 * SIZE(XX)
	movhps	%xmm3,   1 * SIZE(XX)
	addl	 INCX, XX
	ALIGN_3

.L226:
	testl	$2, M
	je	.L227

	movsd	 0 * SIZE(X), %xmm0
	movhps	 1 * SIZE(X), %xmm0
	addl	 INCX, X

	pshufd	$0x4e, %xmm0, %xmm5
	mulpd	%xmm6, %xmm0
	mulpd	%xmm7, %xmm5
	addpd	%xmm5, %xmm0
	movlps	%xmm0,   0 * SIZE(XX)
	movhps	%xmm0,   1 * SIZE(XX)
	addl	 INCX, XX

	movsd	 0 * SIZE(X), %xmm1
	movhps	 1 * SIZE(X), %xmm1
	addl	 INCX, X

	pshufd	$0x4e, %xmm1, %xmm5
	mulpd	%xmm6, %xmm1
	mulpd	%xmm7, %xmm5
	addpd	%xmm5, %xmm1
	movlps	%xmm1,   0 * SIZE(XX)
	movhps	%xmm1,   1 * SIZE(XX)
	addl	 INCX, XX
	ALIGN_3

.L227:
	testl	$1, M
	je	.L999

	movsd	 0 * SIZE(X), %xmm0
	movhps	 1 * SIZE(X), %xmm0

	pshufd	$0x4e, %xmm0, %xmm5
	mulpd	%xmm6, %xmm0
	mulpd	%xmm7, %xmm5
	addpd	%xmm5, %xmm0

	movlps	%xmm0,  0 * SIZE(XX)
	movhps	%xmm0,  1 * SIZE(XX)
	ALIGN_3

.L999:
	xorl	%eax, %eax

	popl	%ebp
	popl	%ebx
	popl	%esi
	popl	%edi
	ret

	EPILOGUE

