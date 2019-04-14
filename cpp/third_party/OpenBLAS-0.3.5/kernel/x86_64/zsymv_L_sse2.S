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
#define PREFETCHSIZE	(16 * 24)
#endif

#ifdef CORE2
#define PREFETCH	prefetcht0
#define PREFETCHW	prefetcht0
#define PREFETCHSIZE	(16 * 24)
#endif

#if defined(PENRYN) || defined(DUNNINGTON)
#define PREFETCH	prefetcht0
#define PREFETCHW	prefetcht0
#define PREFETCHSIZE	(16 * 24)
#endif

#if defined(NEHALEM) || defined(SANDYBRIDGE) || defined(HASWELL) || defined(ZEN) || defined (SKYLAKEX)
#define PREFETCH	prefetcht0
#define PREFETCHW	prefetcht0
#define PREFETCHSIZE	(16 * 24)
#endif

#ifdef PENTIUM4
#define PREFETCH	prefetcht0
#define PREFETCHW	prefetcht0
#define PREFETCHSIZE	(16 * 28)
#endif

#ifdef OPTERON
#define PREFETCH	prefetch
#define PREFETCHW	prefetchw
#define PREFETCHSIZE	(16 * 12)
#define movsd		movlpd
#endif

#if defined(BARCELONA)  || defined(SHANGHAI) || defined(BOBCAT) || defined(BARCELONA_OPTIMIZATION)
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
#define PREFETCHSIZE	(16 * 12)
#endif

#ifndef WINDOWS_ABI

#define STACKSIZE	80

#define OLD_Y		 8 + STACKSIZE(%rsp)
#define OLD_INCY	16 + STACKSIZE(%rsp)
#define OLD_BUFFER	24 + STACKSIZE(%rsp)

#define M	  ARG1
#define	N	  ARG2
#define	A	  ARG3
#define LDA	  ARG4
#define	X	  ARG5
#define INCX	  ARG6

#else

#define STACKSIZE	256

#define OLD_A		 40 + STACKSIZE(%rsp)
#define OLD_LDA		 48 + STACKSIZE(%rsp)
#define OLD_X		 56 + STACKSIZE(%rsp)
#define OLD_INCX	 64 + STACKSIZE(%rsp)
#define OLD_Y		 72 + STACKSIZE(%rsp)
#define OLD_INCY	 80 + STACKSIZE(%rsp)
#define OLD_BUFFER	 88 + STACKSIZE(%rsp)

#define M	  ARG1
#define N	  ARG2
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
#define IS	%r15
#define NEW_X	BUFFER
#define NEW_Y	X

#define ALPHA_R  %xmm0
#define ALPHA_I  %xmm1

#define xtemp1 %xmm0
#define xtemp2 %xmm1
#define xtemp3 %xmm2
#define xtemp4 %xmm3

#define atemp1 %xmm4
#define atemp2 %xmm5
#define atemp3 %xmm6
#define atemp4 %xmm7

#define xsum1  %xmm8
#define xsum2  %xmm9
#define yy1    %xmm10
#define yy2    %xmm11

#define a1     %xmm12
#define a2     %xmm13
#define a3     %xmm14
#define	xt1    %xmm15

#if (defined(HAVE_SSE3) && !defined(CORE_OPTERON)) || defined(BARCELONA) || defined(SHANGHAI) || defined(BARCELONA_OPTIMIZATION)
#define MOVDDUP(a, b, c)	movddup	a(b), c
#define MOVDDUP2(a, b, c)	movddup	a##b, c
#else
#define MOVDDUP(a, b, c)	movlpd	a(b), c;movhpd	a(b), c
#define MOVDDUP2(a, b, c)	movlpd	a##b, c;movhpd	a##b, c
#endif

#ifndef HEMV
#define ADD	addpd
#else
#define ADD	subpd
#endif

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

	movq	OLD_A,     A
	movq	OLD_LDA,   LDA
	movq	OLD_X,     X
	movq	OLD_INCX,  INCX

	movaps	%xmm2, %xmm0
	movaps	%xmm3, %xmm1
#endif

	movq	OLD_Y,     Y
	movq	OLD_INCY,   INCY
	movq	OLD_BUFFER, BUFFER

	salq	$ZBASE_SHIFT, INCX
	salq	$ZBASE_SHIFT, INCY
	salq	$ZBASE_SHIFT, LDA

	testq	M, M
	jle	.L999

	pcmpeqb	%xmm2,  %xmm2
	xorpd	%xmm3,  %xmm3
	psllq	$63,    %xmm2
	unpcklpd %xmm3, %xmm2

	unpcklpd ALPHA_I, ALPHA_R
	unpcklpd ALPHA_R, ALPHA_I
	xorpd	 %xmm2,   ALPHA_I

	movq	BUFFER, XX

	movq	M,  %rax
	sarq	$2, %rax
	jle	.L02
	ALIGN_3

.L01:
	MOVDDUP(0 * SIZE, X, %xmm3)
	MOVDDUP(1 * SIZE, X, %xmm4)
	addq	INCX, X
	MOVDDUP(0 * SIZE, X, %xmm5)
	MOVDDUP(1 * SIZE, X, %xmm6)
	addq	INCX, X

	mulpd	ALPHA_R, %xmm3
	mulpd	ALPHA_I, %xmm4
	mulpd	ALPHA_R, %xmm5
	mulpd	ALPHA_I, %xmm6

	addpd	%xmm4, %xmm3
	addpd	%xmm6, %xmm5

	movapd	%xmm3, 0 * SIZE(XX)
	SHUFPD_1 %xmm3, %xmm3
	pxor	%xmm2, %xmm3
	movapd	%xmm3, 2 * SIZE(XX)

	movapd	%xmm5, 4 * SIZE(XX)
	SHUFPD_1 %xmm5, %xmm5
	pxor	%xmm2, %xmm5
	movapd	%xmm5, 6 * SIZE(XX)

	MOVDDUP(0 * SIZE, X, %xmm3)
	MOVDDUP(1 * SIZE, X, %xmm4)
	addq	INCX, X
	MOVDDUP(0 * SIZE, X, %xmm5)
	MOVDDUP(1 * SIZE, X, %xmm6)
	addq	INCX, X

	mulpd	ALPHA_R, %xmm3
	mulpd	ALPHA_I, %xmm4
	mulpd	ALPHA_R, %xmm5
	mulpd	ALPHA_I, %xmm6

	addpd	%xmm4, %xmm3
	addpd	%xmm6, %xmm5

	movapd	%xmm3,  8 * SIZE(XX)
	SHUFPD_1 %xmm3, %xmm3
	pxor	%xmm2, %xmm3
	movapd	%xmm3, 10 * SIZE(XX)

	movapd	%xmm5, 12 * SIZE(XX)
	SHUFPD_1 %xmm5, %xmm5
	pxor	%xmm2, %xmm5
	movapd	%xmm5, 14 * SIZE(XX)

	subq	$-16 * SIZE, XX
	decq	%rax
	jg	.L01
	ALIGN_3

.L02:
	movq	M, %rax
	andq	$3, %rax
	jle	.L05
	ALIGN_3

.L03:
	MOVDDUP(0 * SIZE, X, %xmm3)
	MOVDDUP(1 * SIZE, X, %xmm4)
	addq	INCX, X

	mulpd	ALPHA_R, %xmm3
	mulpd	ALPHA_I, %xmm4

	addpd	%xmm4, %xmm3

	movapd	%xmm3, 0 * SIZE(XX)
	SHUFPD_1 %xmm3, %xmm3
	pxor	%xmm2, %xmm3
	movapd	%xmm3, 2 * SIZE(XX)

	addq	$4 * SIZE, XX
	decq	%rax
	jg	.L03
	ALIGN_3

.L05:
	/* now we don't need original X */
	movq   Y, NEW_Y

	addq   $512, XX
	andq   $-512, XX

	cmpq   $2 * SIZE, INCY
	je    .L10

	movq   Y,  YY
	movq   XX, NEW_Y

	movq	M,  %rax
	sarq	$2, %rax
	jle	.L07
	ALIGN_3

.L06:
	movsd	0 * SIZE(YY), %xmm0
	movhpd	1 * SIZE(YY), %xmm0
	addq	INCY, YY
	movsd	0 * SIZE(YY), %xmm1
	movhpd	1 * SIZE(YY), %xmm1
	addq	INCY, YY
	movsd	0 * SIZE(YY), %xmm2
	movhpd	1 * SIZE(YY), %xmm2
	addq	INCY, YY
	movsd	0 * SIZE(YY), %xmm3
	movhpd	1 * SIZE(YY), %xmm3
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
	andq	$3, %rax
	jle	.L10
	ALIGN_3

.L08:
	movsd	0 * SIZE(YY), %xmm0
	movhpd	1 * SIZE(YY), %xmm0
	addq	INCY, YY

	movapd	%xmm0, 0 * SIZE(XX)

	addq	$2 * SIZE, XX
	decq	%rax
	jg	.L08
	ALIGN_3

.L10:
	xorq	IS, IS		# is = 0

	cmpq	$2, N
	jl	.L20
	ALIGN_3

.L11:
	movq	A,  A1
	leaq	(A, LDA, 1), A2
	leaq	4 * SIZE(A, LDA, 2), A

	leaq	(, IS, SIZE), I

	leaq	0 * SIZE(NEW_X, I, 4), XX
	leaq	4 * SIZE(NEW_Y, I, 2), YY

	movapd		0 * SIZE(XX), atemp1
	movapd		2 * SIZE(XX), atemp2
	movapd		4 * SIZE(XX), atemp3
	movapd		6 * SIZE(XX), atemp4

	MOVDDUP(0 * SIZE, A1, xsum1)
	MOVDDUP(2 * SIZE, A1, xsum2)

	mulpd	  atemp1, xsum1
	mulpd	  atemp1, xsum2

#ifndef HEMV
	MOVDDUP(1 * SIZE, A1, a1)
	MOVDDUP(3 * SIZE, A1, a2)

	mulpd	  atemp2, a1
	mulpd	  atemp2, a2
	addpd	  a1,     xsum1
	addpd	  a2,     xsum2
#else
	MOVDDUP(3 * SIZE, A1, a2)

	mulpd	  atemp2, a2
	addpd	  a2,     xsum2
#endif

	MOVDDUP(2 * SIZE, A1, a1)
	MOVDDUP(2 * SIZE, A2, a2)

	mulpd	  atemp3, a1
	mulpd	  atemp3, a2
	addpd	  a1,     xsum1
	addpd	  a2,     xsum2

#ifndef HEMV
	MOVDDUP(3 * SIZE, A1, a1)
	MOVDDUP(3 * SIZE, A2, a2)

	mulpd	  atemp4, a1
	mulpd	  atemp4, a2
	addpd	  a1,     xsum1
	addpd	  a2,     xsum2
#else
	MOVDDUP(3 * SIZE, A1, a1)

	mulpd	  atemp4, a1
	subpd	  a1,     xsum1
#endif

	MOVDDUP(4 * SIZE, A1, a1)
	MOVDDUP(6 * SIZE, A2, a2)

	movsd	 0 * SIZE(YY), yy1
	movhpd	 1 * SIZE(YY), yy1
	movsd	 2 * SIZE(YY), yy2
	movhpd	 3 * SIZE(YY), yy2

	movapd	 8 * SIZE(XX), xtemp1
	movapd	10 * SIZE(XX), xtemp2
	movapd	12 * SIZE(XX), xtemp3
	movapd	14 * SIZE(XX), xtemp4

	addq	  $8 * SIZE, XX
	addq	  $4 * SIZE, A1
	addq	  $4 * SIZE, A2

	movq	 M, I
	subq	IS, I
	subq	$2, I
	sarq	$2, I
	jle	.L15
	ALIGN_3

.L12:
	movapd	  xtemp1, xt1
	mulpd	  a1,     xt1
	mulpd	  atemp1, a1
	addpd	  xt1,    xsum1
	addpd	  a1,     yy1
	MOVDDUP(1 * SIZE, A1, a1)

	PREFETCH	PREFETCHSIZE(A1)

	movapd	  xtemp3, xt1
	mulpd	  a2,     xt1
	mulpd	  atemp3, a2
	addpd	  xt1,    xsum2
	addpd	  a2,     yy2
	MOVDDUP(3 * SIZE, A2, a2)

	movapd	  xtemp2, xt1
	mulpd	  a1,     xt1
	mulpd	  atemp2, a1
	ADD	  xt1,    xsum1
	addpd	  a1,     yy1
	MOVDDUP(2 * SIZE, A1, a1)

	movapd	  xtemp4, xt1
	mulpd	  a2,     xt1
	mulpd	  atemp4, a2
	ADD	  xt1,    xsum2
	addpd	  a2,     yy2
	MOVDDUP(0 * SIZE, A2, a2)

	PREFETCH	PREFETCHSIZE(XX)

	movapd	  xtemp3, xt1
	movapd	12 * SIZE(XX), xtemp3
	mulpd	  a1,     xt1
	mulpd	  atemp1, a1
	addpd	  xt1,    xsum1
	addpd	  a1,     yy2
	MOVDDUP(3 * SIZE, A1, a1)

	movapd	  xtemp1, xt1
	movapd	 8 * SIZE(XX), xtemp1
	mulpd	  a2,     xt1
	mulpd	  atemp3, a2
	addpd	  xt1,    xsum2
	addpd	  a2,     yy1
	MOVDDUP(1 * SIZE, A2, a2)

	movapd	  xtemp4, xt1
	movapd	14 * SIZE(XX), xtemp4
	mulpd	  a1,     xt1
	mulpd	  atemp2, a1
	ADD	  xt1,    xsum1
	addpd	  a1,     yy2
	MOVDDUP(4 * SIZE, A1, a1)

	movlpd	 yy2, 2 * SIZE(YY)
	movhpd	 yy2, 3 * SIZE(YY)
	movsd	 6 * SIZE(YY), yy2
	movhpd	 7 * SIZE(YY), yy2

	movapd	  xtemp2, xt1
	movapd	10 * SIZE(XX), xtemp2
	mulpd	  a2,     xt1
	mulpd	  atemp4, a2
	ADD	  xt1,    xsum2
	addpd	  a2,     yy1
	MOVDDUP(6 * SIZE, A2, a2)

	PREFETCH	PREFETCHSIZE(A2)

	movlpd	 yy1, 0 * SIZE(YY)
	movhpd	 yy1, 1 * SIZE(YY)
	movsd	 4 * SIZE(YY), yy1
	movhpd	 5 * SIZE(YY), yy1

	movapd	  xtemp1, xt1
	mulpd	  a1,     xt1
	mulpd	  atemp1, a1
	addpd	  xt1,    xsum1
	addpd	  a1,     yy1
	MOVDDUP(5 * SIZE, A1, a1)

	movapd	  xtemp3, xt1
	mulpd	  a2,     xt1
	mulpd	  atemp3, a2
	addpd	  xt1,    xsum2
	addpd	  a2,     yy2
	MOVDDUP(7 * SIZE, A2, a2)

	movapd	  xtemp2, xt1
	mulpd	  a1,     xt1
	mulpd	  atemp2, a1
	ADD	  xt1,    xsum1
	addpd	  a1,     yy1
	MOVDDUP(6 * SIZE, A1, a1)

	PREFETCHW	PREFETCHSIZE(YY)

	movapd	  xtemp4, xt1
	mulpd	  a2,     xt1
	mulpd	  atemp4, a2
	ADD	  xt1,    xsum2
	addpd	  a2,     yy2
	MOVDDUP(4 * SIZE, A2, a2)

	movapd	  xtemp3, xt1
	movapd	20 * SIZE(XX), xtemp3
	mulpd	  a1,     xt1
	mulpd	  atemp1, a1
	addpd	  xt1,    xsum1
	addpd	  a1,     yy2
	MOVDDUP(7 * SIZE, A1, a1)

	movapd	  xtemp1, xt1
	movapd	16 * SIZE(XX), xtemp1
	mulpd	  a2,     xt1
	mulpd	  atemp3, a2
	addpd	  xt1,    xsum2
	addpd	  a2,     yy1
	MOVDDUP(5 * SIZE, A2, a2)

	movapd	  xtemp4, xt1
	movapd	22 * SIZE(XX), xtemp4
	mulpd	  a1,     xt1
	mulpd	  atemp2, a1
	ADD	  xt1,    xsum1
	addpd	  a1,     yy2
	MOVDDUP( 8 * SIZE, A1, a1)

	movlpd	 yy2, 6 * SIZE(YY)
	movhpd	 yy2, 7 * SIZE(YY)
	movsd	10 * SIZE(YY), yy2
	movhpd	11 * SIZE(YY), yy2

	movapd	  xtemp2, xt1
	movapd	18 * SIZE(XX), xtemp2
	mulpd	  a2,     xt1
	mulpd	  atemp4, a2
	ADD	  xt1,    xsum2
	addpd	  a2,     yy1
	MOVDDUP(10 * SIZE, A2, a2)

	movlpd	 yy1, 4 * SIZE(YY)
	movhpd	 yy1, 5 * SIZE(YY)
	movsd	 8 * SIZE(YY), yy1
	movhpd	 9 * SIZE(YY), yy1

	subq	 $-16 * SIZE, XX
	addq	 $  8 * SIZE, YY
	addq	 $  8 * SIZE, A1
	addq	 $  8 * SIZE, A2

	decq	 I
	jg	 .L12
	ALIGN_3

.L15:
	movq	 M, I
	subq	IS, I
	subq	$2, I
	testq	$2, I
	jle	.L16

	movapd	  xtemp1, xt1
	mulpd	  a1,     xt1
	mulpd	  atemp1, a1
	addpd	  xt1,    xsum1
	addpd	  a1,     yy1
	MOVDDUP(1 * SIZE, A1, a1)

	movapd	  xtemp3, xt1
	mulpd	  a2,     xt1
	mulpd	  atemp3, a2
	addpd	  xt1,    xsum2
	addpd	  a2,     yy2
	MOVDDUP(3 * SIZE, A2, a2)

	movapd	  xtemp2, xt1
	mulpd	  a1,     xt1
	mulpd	  atemp2, a1
	ADD	  xt1,    xsum1
	addpd	  a1,     yy1
	MOVDDUP(2 * SIZE, A1, a1)

	movapd	  xtemp4, xt1
	mulpd	  a2,     xt1
	mulpd	  atemp4, a2
	ADD	  xt1,    xsum2
	addpd	  a2,     yy2
	MOVDDUP(0 * SIZE, A2, a2)

	movapd	  xtemp3, xt1
	movapd	12 * SIZE(XX), xtemp3
	mulpd	  a1,     xt1
	mulpd	  atemp1, a1
	addpd	  xt1,    xsum1
	addpd	  a1,     yy2
	MOVDDUP(3 * SIZE, A1, a1)

	movapd	  xtemp1, xt1
	movapd	 8 * SIZE(XX), xtemp1
	mulpd	  a2,     xt1
	mulpd	  atemp3, a2
	addpd	  xt1,    xsum2
	addpd	  a2,     yy1
	MOVDDUP(1 * SIZE, A2, a2)

	movapd	  xtemp4, xt1
	movapd	14 * SIZE(XX), xtemp4
	mulpd	  a1,     xt1
	mulpd	  atemp2, a1
	ADD	  xt1,    xsum1
	addpd	  a1,     yy2
	MOVDDUP(4 * SIZE, A1, a1)

	movlpd	 yy2, 2 * SIZE(YY)
	movhpd	 yy2, 3 * SIZE(YY)
	movsd	 6 * SIZE(YY), yy2
	movhpd	 7 * SIZE(YY), yy2

	movapd	  xtemp2, xt1
	movapd	10 * SIZE(XX), xtemp2
	mulpd	  a2,     xt1
	mulpd	  atemp4, a2
	ADD	  xt1,    xsum2
	addpd	  a2,     yy1

	movlpd	 yy1, 0 * SIZE(YY)
	movhpd	 yy1, 1 * SIZE(YY)
	movsd	 4 * SIZE(YY), yy1
	movhpd	 5 * SIZE(YY), yy1

	addq	 $4 * SIZE, YY
	addq	 $4 * SIZE, A1
	addq	 $4 * SIZE, A2
	ALIGN_3

.L16:
	testq	$1, M
	jle	.L18

	MOVDDUP(1 * SIZE, A1, a2)

	movapd	  xtemp1, xt1
	mulpd	  a1,     xt1
	mulpd	  atemp1, a1
	addpd	  xt1,    xsum1
	addpd	  a1,     yy1

	MOVDDUP(0 * SIZE, A2, a1)

	movapd	  xtemp2, xt1
	mulpd	  a2,     xt1
	mulpd	  atemp2, a2
	ADD	  xt1,    xsum1
	addpd	  a2,     yy1

	MOVDDUP(1 * SIZE, A2, a2)

	movapd	  xtemp1, xt1
	mulpd	  a1,     xt1
	mulpd	  atemp3, a1
	addpd	  xt1,    xsum2
	addpd	  a1,     yy1

	movapd	  xtemp2, xt1
	mulpd	  a2,     xt1
	mulpd	  atemp4, a2
	ADD	  xt1,    xsum2
	addpd	  a2,     yy1

	movlpd	 yy1, 0 * SIZE(YY)
	movhpd	 yy1, 1 * SIZE(YY)
	ALIGN_3

.L18:
	leaq	(, IS, SIZE), I

	movsd	 0 * SIZE(NEW_Y, I, 2), yy1
	movhpd	 1 * SIZE(NEW_Y, I, 2), yy1
	movsd	 2 * SIZE(NEW_Y, I, 2), yy2
	movhpd	 3 * SIZE(NEW_Y, I, 2), yy2

	addpd	 xsum1, yy1
	addpd	 xsum2, yy2

	movlpd	 yy1, 0 * SIZE(NEW_Y, I, 2)
	movhpd	 yy1, 1 * SIZE(NEW_Y, I, 2)
	movlpd	 yy2, 2 * SIZE(NEW_Y, I, 2)
	movhpd	 yy2, 3 * SIZE(NEW_Y, I, 2)

	addq	 $2, IS

	movq	 IS, I
	addq	 $2, I
	cmpq	 N, I
	jle	 .L11
	ALIGN_3

.L20:
	testq	$1, N
	jle	.L990

	leaq	(, IS, SIZE), I

	movapd	 0 * SIZE(NEW_X, I, 4), atemp1
	movapd	 2 * SIZE(NEW_X, I, 4), atemp2

	movsd	 0 * SIZE(NEW_Y, I, 2), yy1
	movhpd	 1 * SIZE(NEW_Y, I, 2), yy1

#ifndef HEMV
	MOVDDUP(0 * SIZE, A, a1)
	MOVDDUP(1 * SIZE, A, a2)

	mulpd	  atemp1, a1
	mulpd	  atemp2, a2
	addpd	  a1,     yy1
	addpd	  a2,     yy1
#else
	MOVDDUP(0 * SIZE, A, a1)

	mulpd	  atemp1, a1
	addpd	  a1,     yy1
#endif

	movlpd	 yy1, 0 * SIZE(NEW_Y, I, 2)
	movhpd	 yy1, 1 * SIZE(NEW_Y, I, 2)
	ALIGN_3

.L990:
	cmpq   $2 * SIZE, INCY
	je    .L999

	movq	M,  %rax
	sarq	$2, %rax
	jle	.L997
	ALIGN_3

.L996:
	movapd	 0 * SIZE(NEW_Y), %xmm0
	movapd	 2 * SIZE(NEW_Y), %xmm1
	movapd	 4 * SIZE(NEW_Y), %xmm2
	movapd	 6 * SIZE(NEW_Y), %xmm3

	movsd	%xmm0,  0 * SIZE(Y)
	movhpd	%xmm0,  1 * SIZE(Y)
	addq	INCY, Y
	movsd	%xmm1,  0 * SIZE(Y)
	movhpd	%xmm1,  1 * SIZE(Y)
	addq	INCY, Y
	movsd	%xmm2,  0 * SIZE(Y)
	movhpd	%xmm2,  1 * SIZE(Y)
	addq	INCY, Y
	movsd	%xmm3,  0 * SIZE(Y)
	movhpd	%xmm3,  1 * SIZE(Y)
	addq	INCY, Y

	addq	$8 * SIZE, NEW_Y
	decq	%rax
	jg	.L996
	ALIGN_3

.L997:
	movq	M, %rax
	andq	$3, %rax
	jle	.L999
	ALIGN_3

.L998:
	movapd	0 * SIZE(NEW_Y), %xmm0

	movsd	%xmm0,  0 * SIZE(Y)
	movhpd	%xmm0,  1 * SIZE(Y)
	addq	INCY, Y

	addq	$2 * SIZE, NEW_Y

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
