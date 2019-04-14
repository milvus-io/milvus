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

#ifdef ATOM
#define PREFETCH	prefetcht0
#define PREFETCHW	prefetcht0
#define PREFETCHSIZE	(16 * 12)
#endif

#ifdef CORE2
#define PREFETCH	prefetcht0
#define PREFETCHW	prefetcht0
#define PREFETCHSIZE	(16 * 12)
#endif

#if defined(PENRYN) || defined(DUNNINGTON)
#define PREFETCH	prefetcht0
#define PREFETCHW	prefetcht0
#define PREFETCHSIZE	(16 * 12)
#endif

#if defined(NEHALEM) || defined(SANDYBRIDGE)  || defined(HASWELL) || defined(ZEN) || defined (SKYLAKEX)
#define PREFETCH	prefetcht0
#define PREFETCHW	prefetcht0
#define PREFETCHSIZE	(16 * 12)
#endif

#ifdef PENTIUM4
#define PREFETCH	prefetcht0
#define PREFETCHW	prefetcht0
#define PREFETCHSIZE	(16 * 20)
#endif

#ifdef OPTERON
#define PREFETCH	prefetch
#define PREFETCHW	prefetchw
#define PREFETCHSIZE	(16 * 8)
#define movsd		movlps
#endif

#if defined(BARCELONA) || defined(SHANGHAI) || defined(BOBCAT) || defined(BARCELONA_OPTIMIZATION)
#define PREFETCH	prefetch
#define PREFETCHW	prefetchw
#define PREFETCHSIZE	(16 * 16)
#endif

#ifdef NANO
#define PREFETCH	prefetcht0
#define PREFETCHW	prefetcht0
#define PREFETCHSIZE	(16 * 24)
#endif

#ifdef GENERIC
#define PREFETCH	prefetcht0
#define PREFETCHW	prefetcht0
#define PREFETCHSIZE	(16 * 20)
#endif

#ifndef WINDOWS_ABI

#define STACKSIZE	80

#define OLD_Y		 8 + STACKSIZE(%rsp)
#define OLD_INCY	16 + STACKSIZE(%rsp)
#define OLD_BUFFER	24 + STACKSIZE(%rsp)

#define M	  ARG1
#define	IS	  ARG2
#define	A	  ARG3
#define LDA	  ARG4
#define	X	  ARG5
#define INCX	  ARG6

#else

#define STACKSIZE	256

#define OLD_LDA		 40 + STACKSIZE(%rsp)
#define OLD_X		 48 + STACKSIZE(%rsp)
#define OLD_INCX	 56 + STACKSIZE(%rsp)
#define OLD_Y		 64 + STACKSIZE(%rsp)
#define OLD_INCY	 72 + STACKSIZE(%rsp)
#define OLD_BUFFER	 80 + STACKSIZE(%rsp)

#define M	  ARG1
#define IS	  ARG2
#define	A	  ARG4
#define LDA	  ARG3
#define	X	  %rdi
#define INCX	  %rsi

#endif

#define	Y	%r10
#define INCY	%r11
#define BUFFER	%r12

#define TEMP	%rax
#define I	%rax
#define A1	%rbx
#define A2	%rbp
#define XX	%r13
#define YY	%r14
#define NEW_X	BUFFER
#define NEW_Y	X

#define ALPHA  %xmm0

#define atemp1 %xmm0
#define atemp2 %xmm1
#define atemp3 %xmm2
#define atemp4 %xmm3

#define xsum1  %xmm4
#define xsum2  %xmm5
#define xsum3  %xmm6
#define xsum4  %xmm7

#define xtemp1 %xmm8
#define xtemp2 %xmm9
#define yy1    %xmm10
#define	xt1    %xmm11

#define a1     %xmm12
#define a2     %xmm13
#define a3     %xmm14
#define a4     %xmm15


	PROLOGUE
	PROFCODE

	subq	$STACKSIZE, %rsp
	movq	%rbx,  0(%rsp)
	movq	%rbp,  8(%rsp)
	movq	%r12, 16(%rsp)
	movq	%r13, 24(%rsp)
	movq	%r14, 32(%rsp)
	movq	%r15, 40(%rsp)

#ifdef WINDOWS_ABI
	movq	%rdi,    48(%rsp)
	movq	%rsi,    56(%rsp)
	movups	%xmm6,   64(%rsp)
	movups	%xmm7,   80(%rsp)
	movups	%xmm8,   96(%rsp)
	movups	%xmm9,  112(%rsp)
	movups	%xmm10, 128(%rsp)
	movups	%xmm11, 144(%rsp)
	movups	%xmm12, 160(%rsp)
	movups	%xmm13, 176(%rsp)
	movups	%xmm14, 192(%rsp)
	movups	%xmm15, 208(%rsp)

	movq	OLD_LDA,   LDA
	movq	OLD_X,     X
	movq	OLD_INCX,  INCX

	movaps	%xmm2, %xmm0
#endif

	movq	OLD_Y,     Y
	movq	OLD_INCY,   INCY
	movq	OLD_BUFFER, BUFFER

	leaq	(,INCX, SIZE), INCX
	leaq	(,INCY, SIZE), INCY
	leaq	(,LDA,  SIZE), LDA

	testq	M, M
	jle	.L999

	negq	IS
	addq	M, IS

	movq	IS,  TEMP
	imulq	LDA, TEMP
	addq	TEMP, A

	shufps	$0, ALPHA, ALPHA

	movq	BUFFER, XX

	movq	M,  %rax
	sarq	$3, %rax
	jle	.L02
	ALIGN_3

.L01:
	movss	0 * SIZE(X), %xmm1
	addq	INCX, X
	movss	0 * SIZE(X), %xmm2
	addq	INCX, X
	movss	0 * SIZE(X), %xmm3
	addq	INCX, X
	movss	0 * SIZE(X), %xmm4
	addq	INCX, X
	movss	0 * SIZE(X), %xmm5
	addq	INCX, X
	movss	0 * SIZE(X), %xmm6
	addq	INCX, X
	movss	0 * SIZE(X), %xmm7
	addq	INCX, X
	movss	0 * SIZE(X), %xmm8
	addq	INCX, X

	mulss	ALPHA, %xmm1
	mulss	ALPHA, %xmm2
	mulss	ALPHA, %xmm3
	mulss	ALPHA, %xmm4
	mulss	ALPHA, %xmm5
	mulss	ALPHA, %xmm6
	mulss	ALPHA, %xmm7
	mulss	ALPHA, %xmm8

	movss	%xmm1, 0 * SIZE(XX)
	movss	%xmm2, 1 * SIZE(XX)
	movss	%xmm3, 2 * SIZE(XX)
	movss	%xmm4, 3 * SIZE(XX)
	movss	%xmm5, 4 * SIZE(XX)
	movss	%xmm6, 5 * SIZE(XX)
	movss	%xmm7, 6 * SIZE(XX)
	movss	%xmm8, 7 * SIZE(XX)

	addq	$8 * SIZE, XX
	decq	%rax
	jg	.L01
	ALIGN_3

.L02:
	movq	M, %rax
	andq	$7, %rax
	jle	.L05
	ALIGN_3

.L03:
	movss	0 * SIZE(X), %xmm1
	addq	INCX, X

	mulss	ALPHA, %xmm1

	movss	%xmm1, 0 * SIZE(XX)

	addq	$1 * SIZE, XX
	decq	%rax
	jg	.L03
	ALIGN_3

.L05:
	/* now we don't need original X */
	movq   Y, NEW_Y

	addq   $512, XX
	andq   $-512, XX

	cmpq   $SIZE, INCY
	je    .L10

	movq   Y,  YY
	movq   XX, NEW_Y

	movq	M,  %rax
	sarq	$3, %rax
	jle	.L07
	ALIGN_3

.L06:
	movss	0 * SIZE(YY), %xmm0
	addq	INCY, YY
	movss	0 * SIZE(YY), %xmm1
	addq	INCY, YY
	movss	0 * SIZE(YY), %xmm2
	addq	INCY, YY
	movss	0 * SIZE(YY), %xmm3
	addq	INCY, YY
	movss	0 * SIZE(YY), %xmm4
	addq	INCY, YY
	movss	0 * SIZE(YY), %xmm5
	addq	INCY, YY
	movss	0 * SIZE(YY), %xmm6
	addq	INCY, YY
	movss	0 * SIZE(YY), %xmm7
	addq	INCY, YY

	movss	%xmm0, 0 * SIZE(XX)
	movss	%xmm1, 1 * SIZE(XX)
	movss	%xmm2, 2 * SIZE(XX)
	movss	%xmm3, 3 * SIZE(XX)
	movss	%xmm4, 4 * SIZE(XX)
	movss	%xmm5, 5 * SIZE(XX)
	movss	%xmm6, 6 * SIZE(XX)
	movss	%xmm7, 7 * SIZE(XX)

	addq	$8 * SIZE, XX
	decq	%rax
	jg	.L06
	ALIGN_3

.L07:
	movq	M, %rax
	andq	$7, %rax
	jle	.L10
	ALIGN_3

.L08:
	movss	0 * SIZE(YY), %xmm0
	addq	INCY, YY

	movss	%xmm0, 0 * SIZE(XX)

	addq	$1 * SIZE, XX
	decq	%rax
	jg	.L08
	ALIGN_3

.L10:
	movq	 IS, I
	addq	 $4, I
	cmpq	 M,  I
	jg	 .L20
	ALIGN_3

.L11:
	movq	A,  A1
	leaq	(A, LDA, 2), A2
	leaq	(A, LDA, 4), A

	movaps		0 * SIZE(NEW_X, IS, SIZE), atemp4

	pshufd	$0x00, atemp4, atemp1
	pshufd	$0x55, atemp4, atemp2
	pshufd	$0xaa, atemp4, atemp3
	pshufd	$0xff, atemp4, atemp4

	pxor		xsum1, xsum1
	pxor		xsum2, xsum2
	pxor		xsum3, xsum3
	pxor		xsum4, xsum4

	movaps	 0 * SIZE(NEW_X), xtemp1
	movaps	 4 * SIZE(NEW_X), xtemp2

	movsd	 0 * SIZE(A1), a1
	movhps	 2 * SIZE(A1), a1
	movsd	 0 * SIZE(A1, LDA, 1), a2
	movhps	 2 * SIZE(A1, LDA, 1), a2
	movsd	 0 * SIZE(A2), a3
	movhps	 2 * SIZE(A2), a3
	movsd	 0 * SIZE(A2, LDA, 1), a4
	movhps	 2 * SIZE(A2, LDA, 1), a4

	movsd	 0 * SIZE(NEW_Y), yy1
	movhps	 2 * SIZE(NEW_Y), yy1

	movq		NEW_X, XX
	movq		NEW_Y, YY

	movq	IS,  I
	sarq	$4,  I
	jle	.L14
	ALIGN_3

.L12:
	movaps	 xtemp1, xt1
	mulps	 a1,     xt1
	mulps	 atemp1, a1
	addps	 xt1,    xsum1
	addps	 a1,     yy1
	movsd	 4 * SIZE(A1), a1
	movhps	 6 * SIZE(A1), a1

	PREFETCH	PREFETCHSIZE(A1)

	movaps	 xtemp1, xt1
	mulps	 a2,     xt1
	mulps	 atemp2, a2
	addps	 xt1,    xsum2
	addps	 a2,     yy1
	movsd	 4 * SIZE(A1, LDA, 1), a2
	movhps	 6 * SIZE(A1, LDA, 1), a2

	movaps	 xtemp1, xt1
	mulps	 a3,     xt1
	mulps	 atemp3, a3
	addps	 xt1,    xsum3
	addps	 a3,     yy1
	movsd	 4 * SIZE(A2), a3
	movhps	 6 * SIZE(A2), a3

#if !defined(CORE2) && !defined(PENRYN) && !defined(DUNNINGTON)
	PREFETCH	PREFETCHSIZE(XX)
#endif

	movaps	 xtemp1, xt1
	movaps	 8 * SIZE(XX), xtemp1
	mulps	 a4,     xt1
	mulps	 atemp4, a4
	addps	 xt1,    xsum4
	addps	 a4,     yy1
	movsd	 4 * SIZE(A2, LDA, 1), a4
	movhps	 6 * SIZE(A2, LDA, 1), a4

	movlps	 yy1, 0 * SIZE(YY)
	movhps	 yy1, 2 * SIZE(YY)
	movsd	 4 * SIZE(YY), yy1
	movhps	 6 * SIZE(YY), yy1

	movaps	 xtemp2, xt1
	mulps	 a1,     xt1
	mulps	 atemp1, a1
	addps	 xt1,    xsum1
	addps	 a1,     yy1
	movsd	 8 * SIZE(A1), a1
	movhps	10 * SIZE(A1), a1

	PREFETCH	PREFETCHSIZE(A1, LDA, 1)

	movaps	 xtemp2, xt1
	mulps	 a2,     xt1
	mulps	 atemp2, a2
	addps	 xt1,    xsum2
	addps	 a2,     yy1
	movsd	 8 * SIZE(A1, LDA, 1), a2
	movhps	10 * SIZE(A1, LDA, 1), a2

	movaps	 xtemp2, xt1
	mulps	 a3,     xt1
	mulps	 atemp3, a3
	addps	 xt1,    xsum3
	addps	 a3,     yy1
	movsd	 8 * SIZE(A2), a3
	movhps	10 * SIZE(A2), a3

	movaps	 xtemp2, xt1
	movaps	12 * SIZE(XX), xtemp2
	mulps	 a4,     xt1
	mulps	 atemp4, a4
	addps	 xt1,    xsum4
	addps	 a4,     yy1
	movsd	 8 * SIZE(A2, LDA, 1), a4
	movhps	10 * SIZE(A2, LDA, 1), a4

	movlps	 yy1, 4 * SIZE(YY)
	movhps	 yy1, 6 * SIZE(YY)
	movsd	 8 * SIZE(YY), yy1
	movhps	10 * SIZE(YY), yy1


	movaps	 xtemp1, xt1
	mulps	 a1,     xt1
	mulps	 atemp1, a1
	addps	 xt1,    xsum1
	addps	 a1,     yy1
	movsd	12 * SIZE(A1), a1
	movhps	14 * SIZE(A1), a1

	PREFETCH	PREFETCHSIZE(A2)

	movaps	 xtemp1, xt1
	mulps	 a2,     xt1
	mulps	 atemp2, a2
	addps	 xt1,    xsum2
	addps	 a2,     yy1
	movsd	12 * SIZE(A1, LDA, 1), a2
	movhps	14 * SIZE(A1, LDA, 1), a2

	movaps	 xtemp1, xt1
	mulps	 a3,     xt1
	mulps	 atemp3, a3
	addps	 xt1,    xsum3
	addps	 a3,     yy1
	movsd	12 * SIZE(A2), a3
	movhps	14 * SIZE(A2), a3

#if !defined(CORE2) && !defined(PENRYN) && !defined(DUNNINGTON)
	PREFETCHW	PREFETCHSIZE(YY)
#endif

	movaps	 xtemp1, xt1
	movaps	16 * SIZE(XX), xtemp1
	mulps	 a4,     xt1
	mulps	 atemp4, a4
	addps	 xt1,    xsum4
	addps	 a4,     yy1
	movsd	12 * SIZE(A2, LDA, 1), a4
	movhps	14 * SIZE(A2, LDA, 1), a4

	movlps	 yy1,  8 * SIZE(YY)
	movhps	 yy1, 10 * SIZE(YY)
	movsd	12 * SIZE(YY), yy1
	movhps	14 * SIZE(YY), yy1

	movaps	 xtemp2, xt1
	mulps	 a1,     xt1
	mulps	 atemp1, a1
	addps	 xt1,    xsum1
	addps	 a1,     yy1
	movsd	16 * SIZE(A1), a1
	movhps	18 * SIZE(A1), a1

	PREFETCH	PREFETCHSIZE(A2, LDA, 1)

	movaps	 xtemp2, xt1
	mulps	 a2,     xt1
	mulps	 atemp2, a2
	addps	 xt1,    xsum2
	addps	 a2,     yy1
	movsd	16 * SIZE(A1, LDA, 1), a2
	movhps	18 * SIZE(A1, LDA, 1), a2

	movaps	 xtemp2, xt1
	mulps	 a3,     xt1
	mulps	 atemp3, a3
	addps	 xt1,    xsum3
	addps	 a3,     yy1
	movsd	16 * SIZE(A2), a3
	movhps	18 * SIZE(A2), a3

	movaps	 xtemp2, xt1
	movaps	20 * SIZE(XX), xtemp2
	mulps	 a4,     xt1
	mulps	 atemp4, a4
	addps	 xt1,    xsum4
	addps	 a4,     yy1
	movsd	16 * SIZE(A2, LDA, 1), a4
	movhps	18 * SIZE(A2, LDA, 1), a4

	movlps	 yy1, 12 * SIZE(YY)
	movhps	 yy1, 14 * SIZE(YY)
	movsd	16 * SIZE(YY), yy1
	movhps	18 * SIZE(YY), yy1

	addq	 $16 * SIZE, XX
	addq	 $16 * SIZE, YY
	addq	 $16 * SIZE, A1
	addq	 $16 * SIZE, A2

	decq	 I
	jg	 .L12
	ALIGN_3

.L14:
	testq	$8, IS
	jle	.L15

	movaps	 xtemp1, xt1
	mulps	 a1,     xt1
	mulps	 atemp1, a1
	addps	 xt1,    xsum1
	addps	 a1,     yy1
	movsd	 4 * SIZE(A1), a1
	movhps	 6 * SIZE(A1), a1

	movaps	 xtemp1, xt1
	mulps	 a2,     xt1
	mulps	 atemp2, a2
	addps	 xt1,    xsum2
	addps	 a2,     yy1
	movsd	 4 * SIZE(A1, LDA, 1), a2
	movhps	 6 * SIZE(A1, LDA, 1), a2

	movaps	 xtemp1, xt1
	mulps	 a3,     xt1
	mulps	 atemp3, a3
	addps	 xt1,    xsum3
	addps	 a3,     yy1
	movsd	 4 * SIZE(A2), a3
	movhps	 6 * SIZE(A2), a3

	movaps	 xtemp1, xt1
	movaps	 8 * SIZE(XX), xtemp1
	mulps	 a4,     xt1
	mulps	 atemp4, a4
	addps	 xt1,    xsum4
	addps	 a4,     yy1
	movsd	 4 * SIZE(A2, LDA, 1), a4
	movhps	 6 * SIZE(A2, LDA, 1), a4

	movlps	 yy1, 0 * SIZE(YY)
	movhps	 yy1, 2 * SIZE(YY)
	movsd	 4 * SIZE(YY), yy1
	movhps	 6 * SIZE(YY), yy1

	movaps	 xtemp2, xt1
	mulps	 a1,     xt1
	mulps	 atemp1, a1
	addps	 xt1,    xsum1
	addps	 a1,     yy1
	movsd	 8 * SIZE(A1), a1
	movhps	10 * SIZE(A1), a1

	movaps	 xtemp2, xt1
	mulps	 a2,     xt1
	mulps	 atemp2, a2
	addps	 xt1,    xsum2
	addps	 a2,     yy1
	movsd	 8 * SIZE(A1, LDA, 1), a2
	movhps	10 * SIZE(A1, LDA, 1), a2

	movaps	 xtemp2, xt1
	mulps	 a3,     xt1
	mulps	 atemp3, a3
	addps	 xt1,    xsum3
	addps	 a3,     yy1
	movsd	 8 * SIZE(A2), a3
	movhps	10 * SIZE(A2), a3

	movaps	 xtemp2, xt1
	movaps	12 * SIZE(XX), xtemp2
	mulps	 a4,     xt1
	mulps	 atemp4, a4
	addps	 xt1,    xsum4
	addps	 a4,     yy1
	movsd	 8 * SIZE(A2, LDA, 1), a4
	movhps	10 * SIZE(A2, LDA, 1), a4

	movlps	 yy1, 4 * SIZE(YY)
	movhps	 yy1, 6 * SIZE(YY)
	movsd	 8 * SIZE(YY), yy1
	movhps	10 * SIZE(YY), yy1

	addq	 $8 * SIZE, XX
	addq	 $8 * SIZE, YY
	addq	 $8 * SIZE, A1
	addq	 $8 * SIZE, A2
	ALIGN_3

.L15:
	testq	$4, IS
	jle	.L18

	movaps	 xtemp1, xt1
	mulps	 a1,     xt1
	mulps	 atemp1, a1
	addps	 xt1,    xsum1
	addps	 a1,     yy1

	movaps	 xtemp1, xt1
	mulps	 a2,     xt1
	mulps	 atemp2, a2
	addps	 xt1,    xsum2
	addps	 a2,     yy1

	movaps	 xtemp1, xt1
	mulps	 a3,     xt1
	mulps	 atemp3, a3
	addps	 xt1,    xsum3
	addps	 a3,     yy1

	movaps	 xtemp1, xt1
	mulps	 a4,     xt1
	mulps	 atemp4, a4
	addps	 xt1,    xsum4
	addps	 a4,     yy1

	movlps	 yy1, 0 * SIZE(YY)
	movhps	 yy1, 2 * SIZE(YY)
	movsd	 4 * SIZE(YY), yy1
	movhps	 6 * SIZE(YY), yy1

	addq	 $4 * SIZE, XX
	addq	 $4 * SIZE, YY
	addq	 $4 * SIZE, A1
	addq	 $4 * SIZE, A2
	ALIGN_3

.L18:
	movaps		0 * SIZE(NEW_X, IS, SIZE), atemp1

	movss	 0 * SIZE(A1), a1
	movss	 0 * SIZE(A1, LDA, 1), a2
	movss	 0 * SIZE(A2), a3
	movss	 0 * SIZE(A2, LDA, 1), a4

	unpcklps a3, a1
	unpcklps a4, a2
	unpcklps a2, a1

	mulps	 atemp1, a1
	addps	 a1, xsum1

	movsd	 0 * SIZE(A1, LDA, 1), a1
	movss	 1 * SIZE(A2), a2
	movhps	 1 * SIZE(A2, LDA, 1), a2

	shufps	 $0x84, a2, a1

	mulps	 atemp1, a1
	addps	 a1, xsum2

	movsd	 0 * SIZE(A2), a1
	movss	 2 * SIZE(A2), a2
	movhps	 2 * SIZE(A2, LDA, 1), a2

	shufps	 $0x84, a2, a1

	mulps	 atemp1, a1
	addps	 a1, xsum3

	movsd	 0 * SIZE(A2, LDA, 1), a1
	movhps	 2 * SIZE(A2, LDA, 1), a1

	mulps	 atemp1, a1
	addps	 a1, xsum4


#ifndef HAVE_SSE3
	movaps	 xsum1,  xtemp1
	unpcklps xsum3,  xsum1
	unpckhps xsum3,  xtemp1

	movaps	 xsum2,  xtemp2
	unpcklps xsum4,  xsum2
	unpckhps xsum4,  xtemp2

	movaps	 xsum1,  xsum3
	unpcklps xsum2,  xsum1
	unpckhps xsum2,  xsum3

	movaps	 xtemp1, xsum4
	unpcklps xtemp2, xtemp1
	unpckhps xtemp2, xsum4

	addps	 xsum3,  xsum1
	addps	 xtemp1, xsum4
	addps	 xsum4,  xsum1
#else
	haddps	 xsum2, xsum1
	haddps	 xsum4, xsum3

	haddps	 xsum3, xsum1
#endif

	addps	 xsum1, yy1

	movlps	 yy1, 0 * SIZE(YY)
	movhps	 yy1, 2 * SIZE(YY)

	addq	 $4, IS

	movq	 IS, I
	addq	 $4, I
	cmpq	 M, I
	jle	 .L11
	ALIGN_3

.L20:
	testq	$2, M
	jle	.L30

	movq	A,  A1
	leaq	(A, LDA, 2), A

	movsd		0 * SIZE(NEW_X, IS, SIZE), atemp4

	pshufd	$0x00, atemp4, atemp1
	pshufd	$0x55, atemp4, atemp2

	pxor		xsum1, xsum1
	pxor		xsum2, xsum2

	movaps	 0 * SIZE(NEW_X), xtemp1

	movsd	 0 * SIZE(A1), a1
	movhps	 2 * SIZE(A1), a1
	movsd	 0 * SIZE(A1, LDA, 1), a2
	movhps	 2 * SIZE(A1, LDA, 1), a2

	movsd	 0 * SIZE(NEW_Y), yy1
	movhps	 2 * SIZE(NEW_Y), yy1

	movq		NEW_X, XX
	movq		NEW_Y, YY

	movq	IS,  I
	sarq	$2,  I
	jle	.L28
	ALIGN_3

.L22:
	movaps	 xtemp1, xt1
	mulps	 a1,     xt1
	mulps	 atemp1, a1
	addps	 xt1,    xsum1
	addps	 a1,     yy1
	movsd	 4 * SIZE(A1), a1
	movhps	 6 * SIZE(A1), a1

	movaps	 xtemp1, xt1
	movaps	 4 * SIZE(XX), xtemp1
	mulps	 a2,     xt1
	mulps	 atemp2, a2
	addps	 xt1,    xsum2
	addps	 a2,     yy1
	movsd	 4 * SIZE(A1, LDA, 1), a2
	movhps	 6 * SIZE(A1, LDA, 1), a2

	movlps	 yy1, 0 * SIZE(YY)
	movhps	 yy1, 2 * SIZE(YY)
	movsd	 4 * SIZE(YY), yy1
	movhps	 6 * SIZE(YY), yy1

	addq	 $4 * SIZE, XX
	addq	 $4 * SIZE, YY
	addq	 $4 * SIZE, A1

	decq	 I
	jg	 .L22
	ALIGN_3

.L28:
	movsd		0 * SIZE(NEW_X, IS, SIZE), atemp1

	movss	 0 * SIZE(A1), a1
	movss	 0 * SIZE(A1, LDA, 1), a2

	unpcklps a2, a1

	mulps	 atemp1, a1
	addps	 a1, xsum1

	movsd	 0 * SIZE(A1, LDA, 1), a1
	mulps	 atemp1, a1
	addps	 a1, xsum2

#ifndef HAVE_SSE3
	movhlps	 xsum1, xsum3
	movhlps	 xsum2, xsum4
	addps	 xsum3, xsum1
	addps	 xsum4, xsum2

	unpcklps xsum2, xsum1
	movhlps	 xsum1, xsum2

	addps	 xsum2, xsum1
#else
	haddps	 xsum2, xsum1
	haddps	 xsum1, xsum1
#endif

	addps	 xsum1, yy1

	movlps	 yy1, 0 * SIZE(YY)

	addq	 $2, IS
	ALIGN_3

.L30:
	testq	$1, M
	jle	.L990

	movq	A,  A1

	movss		0 * SIZE(NEW_X, IS, SIZE), atemp1

	pshufd	$0x00, atemp1, atemp1

	pxor		xsum1, xsum1
	pxor		xsum2, xsum2

	movss	 0 * SIZE(NEW_Y), yy1

	movss	 0 * SIZE(NEW_X), xtemp1
	movss	 1 * SIZE(NEW_X), xtemp2

	movss	 0 * SIZE(A1), a1
	movss	 1 * SIZE(A1), a2

	movq		NEW_X, XX
	movq		NEW_Y, YY

	movq	IS,  I
	sarq	$1,  I
	jle	.L38
	ALIGN_3

.L32:
	movaps	 xtemp1, xt1
	movss	 2 * SIZE(XX), xtemp1
	mulps	 a1,     xt1
	mulps	 atemp1, a1
	addps	 xt1,    xsum1
	addps	 a1,     yy1
	movss	 2 * SIZE(A1), a1

	movss	 yy1, 0 * SIZE(YY)
	movss	 1 * SIZE(YY), yy1

	movaps	 xtemp2, xt1
	movss	 3 * SIZE(XX), xtemp2
	mulps	 a2,     xt1
	mulps	 atemp1, a2
	addps	 xt1,    xsum1
	addps	 a2,     yy1
	movss	 3 * SIZE(A1), a2

	movss	 yy1, 1 * SIZE(YY)
	movss	 2 * SIZE(YY), yy1

	addq	 $2 * SIZE, XX
	addq	 $2 * SIZE, YY
	addq	 $2 * SIZE, A1

	decq	 I
	jg	 .L32
	ALIGN_3

.L38:
	movsd		0 * SIZE(NEW_X, IS, SIZE), atemp1

	movss	 0 * SIZE(A1), a1
	mulss	 atemp1, a1
	addss	 a1, xsum1

#ifndef HAVE_SSE3
	movhlps	 xsum1, xsum3
	movhlps	 xsum2, xsum4
	addps	 xsum3, xsum1
	addps	 xsum4, xsum2

	unpcklps xsum2, xsum1
	movhlps	 xsum1, xsum2

	addps	 xsum2, xsum1
#else
	addss	 xsum2, xsum1
#endif

	addss	 xsum1, yy1

	movss	 yy1, 0 * SIZE(YY)

	addq	 $2, IS
	ALIGN_3

.L990:
	cmpq   $SIZE, INCY
	je    .L999

	movq	M,  %rax
	sarq	$3, %rax
	jle	.L997
	ALIGN_3

.L996:
	movss	 0 * SIZE(NEW_Y), %xmm0
	movss	 1 * SIZE(NEW_Y), %xmm1
	movss	 2 * SIZE(NEW_Y), %xmm2
	movss	 3 * SIZE(NEW_Y), %xmm3
	movss	 4 * SIZE(NEW_Y), %xmm4
	movss	 5 * SIZE(NEW_Y), %xmm5
	movss	 6 * SIZE(NEW_Y), %xmm6
	movss	 7 * SIZE(NEW_Y), %xmm7

	movss	%xmm0,  0 * SIZE(Y)
	addq	INCY, Y
	movss	%xmm1,  0 * SIZE(Y)
	addq	INCY, Y
	movss	%xmm2,  0 * SIZE(Y)
	addq	INCY, Y
	movss	%xmm3,  0 * SIZE(Y)
	addq	INCY, Y
	movss	%xmm4,  0 * SIZE(Y)
	addq	INCY, Y
	movss	%xmm5,  0 * SIZE(Y)
	addq	INCY, Y
	movss	%xmm6,  0 * SIZE(Y)
	addq	INCY, Y
	movss	%xmm7,  0 * SIZE(Y)
	addq	INCY, Y

	addq	$8 * SIZE, NEW_Y
	decq	%rax
	jg	.L996
	ALIGN_3

.L997:
	movq	M, %rax
	andq	$7, %rax
	jle	.L999
	ALIGN_3

.L998:
	movss	0 * SIZE(NEW_Y), %xmm0

	movss	%xmm0,  0 * SIZE(Y)
	addq	INCY, Y

	addq	$1 * SIZE, NEW_Y

	decq	%rax
	jg	.L998
	ALIGN_3


.L999:
	movq	  0(%rsp), %rbx
	movq	  8(%rsp), %rbp
	movq	 16(%rsp), %r12
	movq	 24(%rsp), %r13
	movq	 32(%rsp), %r14
	movq	 40(%rsp), %r15

#ifdef WINDOWS_ABI
	movq	 48(%rsp), %rdi
	movq	 56(%rsp), %rsi
	movups	 64(%rsp), %xmm6
	movups	 80(%rsp), %xmm7
	movups	 96(%rsp), %xmm8
	movups	112(%rsp), %xmm9
	movups	128(%rsp), %xmm10
	movups	144(%rsp), %xmm11
	movups	160(%rsp), %xmm12
	movups	176(%rsp), %xmm13
	movups	192(%rsp), %xmm14
	movups	208(%rsp), %xmm15
#endif

	addq	$STACKSIZE, %rsp
	ret
	EPILOGUE
