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
#define PREFETCHSIZE	(16 * 24)
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
#define movsd		movlpd
#endif

#if defined(BARCELONA) || defined(SHANGHAI) || defined(BOBCAT) || defined(BARCELONA_OPTIMIZATION)
#define PREFETCH	prefetch
#define PREFETCHW	prefetchw
#define PREFETCHSIZE	(16 * 16)
#endif

#ifdef NANO
#define PREFETCH	prefetcht0
#define PREFETCHW	prefetcht0
#define PREFETCHSIZE	(8 * 24)
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

#define xtemp1 %xmm0
#define xtemp2 %xmm1
#define yy1    %xmm2
#define yy2    %xmm3

#define atemp1 %xmm4
#define atemp2 %xmm5
#define atemp3 %xmm6
#define atemp4 %xmm7

#define xsum1  %xmm8
#define xsum2  %xmm9
#define xsum3  %xmm10
#define xsum4  %xmm11

#define a1     %xmm12
#define a2     %xmm13
#define a3     %xmm14
#define	xt1    %xmm15

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

	unpcklpd ALPHA, ALPHA

	movq	BUFFER, XX

	movq	M,  %rax
	sarq	$3, %rax
	jle	.L02
	ALIGN_3

.L01:
	movsd	0 * SIZE(X), %xmm1
	addq	INCX, X
	movhpd	0 * SIZE(X), %xmm1
	addq	INCX, X
	movsd	0 * SIZE(X), %xmm2
	addq	INCX, X
	movhpd	0 * SIZE(X), %xmm2
	addq	INCX, X
	movsd	0 * SIZE(X), %xmm3
	addq	INCX, X
	movhpd	0 * SIZE(X), %xmm3
	addq	INCX, X
	movsd	0 * SIZE(X), %xmm4
	addq	INCX, X
	movhpd	0 * SIZE(X), %xmm4
	addq	INCX, X

	mulpd	ALPHA, %xmm1
	mulpd	ALPHA, %xmm2
	mulpd	ALPHA, %xmm3
	mulpd	ALPHA, %xmm4

	movapd	%xmm1, 0 * SIZE(XX)
	movapd	%xmm2, 2 * SIZE(XX)
	movapd	%xmm3, 4 * SIZE(XX)
	movapd	%xmm4, 6 * SIZE(XX)

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
	movsd	0 * SIZE(X), %xmm1
	addq	INCX, X

	mulsd	ALPHA, %xmm1

	movlpd	%xmm1, 0 * SIZE(XX)

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
	movsd	0 * SIZE(YY), %xmm0
	addq	INCY, YY
	movhpd	0 * SIZE(YY), %xmm0
	addq	INCY, YY
	movsd	0 * SIZE(YY), %xmm1
	addq	INCY, YY
	movhpd	0 * SIZE(YY), %xmm1
	addq	INCY, YY
	movsd	0 * SIZE(YY), %xmm2
	addq	INCY, YY
	movhpd	0 * SIZE(YY), %xmm2
	addq	INCY, YY
	movsd	0 * SIZE(YY), %xmm3
	addq	INCY, YY
	movhpd	0 * SIZE(YY), %xmm3
	addq	INCY, YY

	movapd	%xmm0, 0 * SIZE(XX)
	movapd	%xmm1, 2 * SIZE(XX)
	movapd	%xmm2, 4 * SIZE(XX)
	movapd	%xmm3, 6 * SIZE(XX)

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
	movsd	0 * SIZE(YY), %xmm0
	addq	INCY, YY

	movsd	%xmm0, 0 * SIZE(XX)

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

#ifdef HAVE_SSE3
	movddup		0 * SIZE(NEW_X, IS, SIZE), atemp1
	movddup		1 * SIZE(NEW_X, IS, SIZE), atemp2
	movddup		2 * SIZE(NEW_X, IS, SIZE), atemp3
	movddup		3 * SIZE(NEW_X, IS, SIZE), atemp4
#else
	movsd		0 * SIZE(NEW_X, IS, SIZE), atemp1
	movhpd		0 * SIZE(NEW_X, IS, SIZE), atemp1
	movsd		1 * SIZE(NEW_X, IS, SIZE), atemp2
	movhpd		1 * SIZE(NEW_X, IS, SIZE), atemp2
	movsd		2 * SIZE(NEW_X, IS, SIZE), atemp3
	movhpd		2 * SIZE(NEW_X, IS, SIZE), atemp3
	movsd		3 * SIZE(NEW_X, IS, SIZE), atemp4
	movhpd		3 * SIZE(NEW_X, IS, SIZE), atemp4
#endif

	pxor		xsum1, xsum1
	pxor		xsum2, xsum2
	pxor		xsum3, xsum3
	pxor		xsum4, xsum4

	movapd	 0 * SIZE(NEW_X), xtemp1
	movapd	 2 * SIZE(NEW_X), xtemp2

	movsd	 0 * SIZE(A1), a1
	movhpd	 1 * SIZE(A1), a1
	movsd	 2 * SIZE(A1), a2
	movhpd	 3 * SIZE(A1), a2
	movsd	 0 * SIZE(A1, LDA, 1), a3
	movhpd	 1 * SIZE(A1, LDA, 1), a3

	movsd	 0 * SIZE(NEW_Y), yy1
	movhpd	 1 * SIZE(NEW_Y), yy1
	movsd	 2 * SIZE(NEW_Y), yy2
	movhpd	 3 * SIZE(NEW_Y), yy2

	movq		NEW_X, XX
	movq		NEW_Y, YY

	movq	IS,  I
	sarq	$3,  I
	jle	.L15
	ALIGN_3

.L12:
	movapd	 xtemp1, xt1
	mulpd	 a1,     xt1
	mulpd	 atemp1, a1
	addpd	 xt1,    xsum1
	addpd	 a1,     yy1
	movsd	 2 * SIZE(A1, LDA, 1), a1
	movhpd	 3 * SIZE(A1, LDA, 1), a1

	PREFETCH	PREFETCHSIZE(A1)

	movapd	 xtemp2, xt1
	mulpd	 a2,     xt1
	mulpd	 atemp1, a2
	addpd	 xt1,    xsum1
	addpd	 a2,     yy2
	movsd	 0 * SIZE(A2), a2
	movhpd	 1 * SIZE(A2), a2

	movapd	 xtemp1, xt1
	mulpd	 a3,     xt1
	mulpd	 atemp2, a3
	addpd	 xt1,    xsum2
	addpd	 a3,     yy1
	movsd	 2 * SIZE(A2), a3
	movhpd	 3 * SIZE(A2), a3

#if !defined(CORE2) && !defined(PENRYN) && !defined(DUNNINGTON)
	PREFETCH	PREFETCHSIZE(XX)
#endif

	movapd	 xtemp2, xt1
	mulpd	 a1,     xt1
	mulpd	 atemp2, a1
	addpd	 xt1,    xsum2
	addpd	 a1,     yy2
	movsd	 0 * SIZE(A2, LDA, 1), a1
	movhpd	 1 * SIZE(A2, LDA, 1), a1

	movapd	 xtemp1, xt1
	mulpd	 a2,     xt1
	mulpd	 atemp3, a2
	addpd	 xt1,    xsum3
	addpd	 a2,     yy1
	movsd	 2 * SIZE(A2, LDA, 1), a2
	movhpd	 3 * SIZE(A2, LDA, 1), a2

	PREFETCH	PREFETCHSIZE(A1, LDA, 1)

	movapd	 xtemp2, xt1
	mulpd	 a3,     xt1
	mulpd	 atemp3, a3
	addpd	 xt1,    xsum3
	addpd	 a3,     yy2
	movsd	 4 * SIZE(A1), a3
	movhpd	 5 * SIZE(A1), a3

	movapd	 xtemp1, xt1
	movapd	 4 * SIZE(XX), xtemp1
	mulpd	 a1,     xt1
	mulpd	 atemp4, a1
	addpd	 xt1,    xsum4
	addpd	 a1,     yy1
	movsd	 6 * SIZE(A1), a1
	movhpd	 7 * SIZE(A1), a1

	movapd	 xtemp2, xt1
	movapd	 6 * SIZE(XX), xtemp2
	mulpd	 a2,     xt1
	mulpd	 atemp4, a2
	addpd	 xt1,    xsum4
	addpd	 a2,     yy2
	movsd	 4 * SIZE(A1, LDA, 1), a2
	movhpd	 5 * SIZE(A1, LDA, 1), a2

	movsd	 yy1, 0 * SIZE(YY)
	movhpd	 yy1, 1 * SIZE(YY)
	movsd	 4 * SIZE(YY), yy1
	movhpd	 5 * SIZE(YY), yy1

	movsd	 yy2, 2 * SIZE(YY)
	movhpd	 yy2, 3 * SIZE(YY)
	movsd	 6 * SIZE(YY), yy2
	movhpd	 7 * SIZE(YY), yy2

	movapd	 xtemp1, xt1
	mulpd	 a3,     xt1
	mulpd	 atemp1, a3
	addpd	 xt1,    xsum1
	addpd	 a3,     yy1
	movsd	 6 * SIZE(A1, LDA, 1), a3
	movhpd	 7 * SIZE(A1, LDA, 1), a3

	PREFETCH	PREFETCHSIZE(A2)

	movapd	 xtemp2, xt1
	mulpd	 a1,     xt1
	mulpd	 atemp1, a1
	addpd	 xt1,    xsum1
	addpd	 a1,     yy2
	movsd	 4 * SIZE(A2), a1
	movhpd	 5 * SIZE(A2), a1

	movapd	 xtemp1, xt1
	mulpd	 a2,     xt1
	mulpd	 atemp2, a2
	addpd	 xt1,    xsum2
	addpd	 a2,     yy1
	movsd	 6 * SIZE(A2), a2
	movhpd	 7 * SIZE(A2), a2

#if !defined(CORE2) && !defined(PENRYN) && !defined(DUNNINGTON)
	PREFETCHW	PREFETCHSIZE(YY)
#endif

	movapd	 xtemp2, xt1
	mulpd	 a3,     xt1
	mulpd	 atemp2, a3
	addpd	 xt1,    xsum2
	addpd	 a3,     yy2
	movsd	 4 * SIZE(A2, LDA, 1), a3
	movhpd	 5 * SIZE(A2, LDA, 1), a3

	movapd	 xtemp1, xt1
	mulpd	 a1,     xt1
	mulpd	 atemp3, a1
	addpd	 xt1,    xsum3
	addpd	 a1,     yy1
	movsd	 6 * SIZE(A2, LDA, 1), a1
	movhpd	 7 * SIZE(A2, LDA, 1), a1

	PREFETCH	PREFETCHSIZE(A2, LDA, 1)

	movapd	 xtemp2, xt1
	mulpd	 a2,     xt1
	mulpd	 atemp3, a2
	addpd	 xt1,    xsum3
	addpd	 a2,     yy2
	movsd	10 * SIZE(A1), a2
	movhpd	11 * SIZE(A1), a2

	movapd	 xtemp1, xt1
	movapd	 8 * SIZE(XX), xtemp1
	mulpd	 a3,     xt1
	mulpd	 atemp4, a3
	addpd	 xt1,    xsum4
	addpd	 a3,     yy1
	movsd	 8 * SIZE(A1, LDA, 1), a3
	movhpd	 9 * SIZE(A1, LDA, 1), a3

	movapd	 xtemp2, xt1
	movapd	10 * SIZE(XX), xtemp2
	mulpd	 a1,     xt1
	mulpd	 atemp4, a1
	addpd	 xt1,    xsum4
	addpd	 a1,     yy2
	movsd	 8 * SIZE(A1), a1
	movhpd	 9 * SIZE(A1), a1

	movsd	 yy1, 4 * SIZE(YY)
	movhpd	 yy1, 5 * SIZE(YY)
	movsd	 8 * SIZE(YY), yy1
	movhpd	 9 * SIZE(YY), yy1

	movsd	 yy2, 6 * SIZE(YY)
	movhpd	 yy2, 7 * SIZE(YY)
	movsd	10 * SIZE(YY), yy2
	movhpd	11 * SIZE(YY), yy2

	addq	 $8 * SIZE, XX
	addq	 $8 * SIZE, YY
	addq	 $8 * SIZE, A1
	addq	 $8 * SIZE, A2

	decq	 I
	jg	 .L12
	ALIGN_3

.L15:
	testq	$4, IS
	jle	.L18

	movapd	 xtemp1, xt1
	mulpd	 a1,     xt1
	mulpd	 atemp1, a1
	addpd	 xt1,    xsum1
	addpd	 a1,     yy1
	movsd	 2 * SIZE(A1, LDA, 1), a1
	movhpd	 3 * SIZE(A1, LDA, 1), a1

	movapd	 xtemp2, xt1
	mulpd	 a2,     xt1
	mulpd	 atemp1, a2
	addpd	 xt1,    xsum1
	addpd	 a2,     yy2
	movsd	 0 * SIZE(A2), a2
	movhpd	 1 * SIZE(A2), a2

	movapd	 xtemp1, xt1
	mulpd	 a3,     xt1
	mulpd	 atemp2, a3
	addpd	 xt1,    xsum2
	addpd	 a3,     yy1
	movsd	 2 * SIZE(A2), a3
	movhpd	 3 * SIZE(A2), a3

	movapd	 xtemp2, xt1
	mulpd	 a1,     xt1
	mulpd	 atemp2, a1
	addpd	 xt1,    xsum2
	addpd	 a1,     yy2
	movsd	 0 * SIZE(A2, LDA, 1), a1
	movhpd	 1 * SIZE(A2, LDA, 1), a1

	movapd	 xtemp1, xt1
	mulpd	 a2,     xt1
	mulpd	 atemp3, a2
	addpd	 xt1,    xsum3
	addpd	 a2,     yy1
	movsd	 2 * SIZE(A2, LDA, 1), a2
	movhpd	 3 * SIZE(A2, LDA, 1), a2

	movapd	 xtemp2, xt1
	mulpd	 a3,     xt1
	mulpd	 atemp3, a3
	addpd	 xt1,    xsum3
	addpd	 a3,     yy2

	movapd	 xtemp1, xt1
	movapd	 4 * SIZE(XX), xtemp1
	mulpd	 a1,     xt1
	mulpd	 atemp4, a1
	addpd	 xt1,    xsum4
	addpd	 a1,     yy1

	movapd	 xtemp2, xt1
	movapd	 6 * SIZE(XX), xtemp2
	mulpd	 a2,     xt1
	mulpd	 atemp4, a2
	addpd	 xt1,    xsum4
	addpd	 a2,     yy2

	movsd	 yy1, 0 * SIZE(YY)
	movhpd	 yy1, 1 * SIZE(YY)
	movsd	 4 * SIZE(YY), yy1
	movhpd	 5 * SIZE(YY), yy1

	movsd	 yy2, 2 * SIZE(YY)
	movhpd	 yy2, 3 * SIZE(YY)
	movsd	 6 * SIZE(YY), yy2
	movhpd	 7 * SIZE(YY), yy2

	addq	 $4 * SIZE, XX
	addq	 $4 * SIZE, YY
	addq	 $4 * SIZE, A1
	addq	 $4 * SIZE, A2
	ALIGN_3

.L18:
	unpckhpd atemp2, atemp1
	unpckhpd atemp4, atemp3

	movsd	 0 * SIZE(A1), a1
	movhpd	 0 * SIZE(A1, LDA, 1), a1
	mulpd	 atemp1, a1
	addpd	 a1, xsum1

	movsd	 0 * SIZE(A1, LDA, 1), a1
	movhpd	 1 * SIZE(A1, LDA, 1), a1
	mulpd	 atemp1, a1
	addpd	 a1, xsum2

	movsd	 0 * SIZE(A2), a1
	movhpd	 1 * SIZE(A2), a1
	mulpd	 atemp1, a1
	addpd	 a1, xsum3

	movsd	 0 * SIZE(A2, LDA, 1), a1
	movhpd	 1 * SIZE(A2, LDA, 1), a1
	mulpd	 atemp1, a1
	addpd	 a1, xsum4

	movsd	 0 * SIZE(A2), a1
	movhpd	 0 * SIZE(A2, LDA, 1), a1
	mulpd	 atemp3, a1
	addpd	 a1, xsum1

	movsd	 1 * SIZE(A2), a1
	movhpd	 1 * SIZE(A2, LDA, 1), a1
	mulpd	 atemp3, a1
	addpd	 a1, xsum2

	movsd	 2 * SIZE(A2), a1
	movhpd	 2 * SIZE(A2, LDA, 1), a1
	mulpd	 atemp3, a1
	addpd	 a1, xsum3

	movsd	 2 * SIZE(A2, LDA, 1), a1
	movhpd	 3 * SIZE(A2, LDA, 1), a1
	mulpd	 atemp3, a1
	addpd	 a1, xsum4

#ifndef HAVE_SSE3
	movapd	xsum1, atemp1
	movapd	xsum3, atemp3

	unpcklpd xsum2, xsum1
	unpcklpd xsum4, xsum3

	unpckhpd xsum2, atemp1
	unpckhpd xsum4, atemp3

	addpd	 atemp1, xsum1
	addpd	 atemp3, xsum3
#else
	haddpd	 xsum2, xsum1
	haddpd	 xsum4, xsum3
#endif

	addpd	 xsum1, yy1
	addpd	 xsum3, yy2

	movsd	 yy1, 0 * SIZE(YY)
	movhpd	 yy1, 1 * SIZE(YY)
	movsd	 yy2, 2 * SIZE(YY)
	movhpd	 yy2, 3 * SIZE(YY)

	addq	 $4, IS

	movq	 IS, I
	addq	 $4, I
	cmpq	 M, I
	jle	 .L11
	ALIGN_3

.L20:
	testq	$2, M
	je	.L30
	ALIGN_3

.L21:
	movq	A,  A1
	leaq	(A, LDA, 2), A

#ifdef HAVE_SSE3
	movddup		0 * SIZE(NEW_X, IS, SIZE), atemp1
	movddup		1 * SIZE(NEW_X, IS, SIZE), atemp2
#else
	movsd		0 * SIZE(NEW_X, IS, SIZE), atemp1
	movhpd		0 * SIZE(NEW_X, IS, SIZE), atemp1
	movsd		1 * SIZE(NEW_X, IS, SIZE), atemp2
	movhpd		1 * SIZE(NEW_X, IS, SIZE), atemp2
#endif

	pxor		xsum1, xsum1
	pxor		xsum2, xsum2

	movapd	 0 * SIZE(NEW_X), xtemp1

	movsd	 0 * SIZE(NEW_Y), yy1
	movhpd	 1 * SIZE(NEW_Y), yy1

	movsd	 0 * SIZE(A1), a1
	movhpd	 1 * SIZE(A1), a1
	movsd	 0 * SIZE(A1, LDA, 1), a2
	movhpd	 1 * SIZE(A1, LDA, 1), a2

	movq		NEW_X, XX
	movq		NEW_Y, YY

	movq	IS,  I
	sarq	$1,  I
	jle	.L28
	ALIGN_3

.L22:
	movapd	 xtemp1, xt1
	mulpd	 a1,     xt1
	mulpd	 atemp1, a1
	addpd	 xt1,    xsum1
	addpd	 a1,     yy1
	movsd	 2 * SIZE(A1), a1
	movhpd	 3 * SIZE(A1), a1

	movapd	 xtemp1, xt1
	movapd	 2 * SIZE(XX), xtemp1
	mulpd	 a2,     xt1
	mulpd	 atemp2, a2
	addpd	 xt1,    xsum2
	addpd	 a2,     yy1
	movsd	 2 * SIZE(A1, LDA, 1), a2
	movhpd	 3 * SIZE(A1, LDA, 1), a2

	movsd	 yy1, 0 * SIZE(YY)
	movhpd	 yy1, 1 * SIZE(YY)
	movsd	 2 * SIZE(YY), yy1
	movhpd	 3 * SIZE(YY), yy1

	addq	 $2 * SIZE, XX
	addq	 $2 * SIZE, YY
	addq	 $2 * SIZE, A1

	decq	 I
	jg	 .L22
	ALIGN_3

.L28:
	unpckhpd atemp2, atemp1

	movsd	 0 * SIZE(A1), a1
	movhpd	 0 * SIZE(A1, LDA, 1), a1
	mulpd	 atemp1, a1
	addpd	 a1, xsum1

	movsd	 0 * SIZE(A1, LDA, 1), a1
	movhpd	 1 * SIZE(A1, LDA, 1), a1
	mulpd	 atemp1, a1
	addpd	 a1, xsum2

#ifndef HAVE_SSE3
	movapd	xsum1, atemp1

	unpcklpd xsum2, xsum1
	unpckhpd xsum2, atemp1

	addpd	 atemp1, xsum1
#else
	haddpd	 xsum2, xsum1
#endif

	addpd	 xsum1, yy1

	movsd	 yy1, 0 * SIZE(YY)
	movhpd	 yy1, 1 * SIZE(YY)

	addq	 $2, IS
	ALIGN_3

.L30:
	testq	$1, M
	je	.L990
	ALIGN_3

.L31:
	movq	A,  A1

#ifdef HAVE_SSE3
	movddup		0 * SIZE(NEW_X, IS, SIZE), atemp1
#else
	movsd		0 * SIZE(NEW_X, IS, SIZE), atemp1
	movhpd		0 * SIZE(NEW_X, IS, SIZE), atemp1
#endif

	pxor		xsum1, xsum1

	movsd	 0 * SIZE(NEW_X), xtemp1
	movsd	 0 * SIZE(NEW_Y), yy1
	movsd	 0 * SIZE(A1), a1

	movq		NEW_X, XX
	movq		NEW_Y, YY

	movq	IS,  I
	testq	I,  I
	jle	.L38
	ALIGN_3

.L32:
	movapd	 xtemp1, xt1
	mulpd	 a1,     xt1
	mulpd	 atemp1, a1
	addpd	 xt1,    xsum1
	addpd	 a1,     yy1
	movsd	 1 * SIZE(A1), a1

	movsd	 1 * SIZE(XX), xtemp1

	movsd	 yy1, 0 * SIZE(YY)
	movsd	 1 * SIZE(YY), yy1

	addq	 $1 * SIZE, XX
	addq	 $1 * SIZE, YY
	addq	 $1 * SIZE, A1

	decq	 I
	jg	 .L32
	ALIGN_3

.L38:
	movsd	 0 * SIZE(A1), a1
	mulsd	 atemp1, a1
	addsd	 a1, xsum1

	addsd	 xsum1, yy1

	movsd	 yy1, 0 * SIZE(YY)
	ALIGN_3

.L990:
	cmpq   $SIZE, INCY
	je    .L999

	movq	M,  %rax
	sarq	$3, %rax
	jle	.L997
	ALIGN_3

.L996:
	movapd	 0 * SIZE(NEW_Y), %xmm0
	movapd	 2 * SIZE(NEW_Y), %xmm1
	movapd	 4 * SIZE(NEW_Y), %xmm2
	movapd	 6 * SIZE(NEW_Y), %xmm3

	movsd	%xmm0,  0 * SIZE(Y)
	addq	INCY, Y
	movhpd	%xmm0,  0 * SIZE(Y)
	addq	INCY, Y
	movsd	%xmm1,  0 * SIZE(Y)
	addq	INCY, Y
	movhpd	%xmm1,  0 * SIZE(Y)
	addq	INCY, Y
	movsd	%xmm2,  0 * SIZE(Y)
	addq	INCY, Y
	movhpd	%xmm2,  0 * SIZE(Y)
	addq	INCY, Y
	movsd	%xmm3,  0 * SIZE(Y)
	addq	INCY, Y
	movhpd	%xmm3,  0 * SIZE(Y)
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
	movsd	0 * SIZE(NEW_Y), %xmm0

	movsd	%xmm0,  0 * SIZE(Y)
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
