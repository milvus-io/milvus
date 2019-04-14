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

#if defined(NEHALEM) || defined(SANDYBRIDGE)  || defined(HASWELL) || defined(ZEN) || defined (SKYLAKEX)
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
#define PREFETCHSIZE	(16 * 24)
#endif

#ifdef GENERIC
#define PREFETCH	prefetcht0
#define PREFETCHW	prefetcht0
#define PREFETCHSIZE	(16 * 14)
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

#define xsum1  %xmm0
#define xsum2  %xmm1
#define xsum3  %xmm2
#define xsum4  %xmm3

#define atemp1 %xmm4
#define atemp2 %xmm5
#define atemp3 %xmm6
#define atemp4 %xmm7

#define xtemp1 %xmm8
#define xtemp2 %xmm9
#define a1     %xmm10
#define a2     %xmm11

#define a3     %xmm12
#define yy1    %xmm13
#define	xt1    %xmm14
#define	xt2    %xmm15

#if (defined(HAVE_SSE3) && !defined(CORE_OPTERON)) || defined(BARCELONA) || defined(SHANGHAI) || defined(BARCELONA_OPTIMIZATION)
#define MOVDDUP(a, b, c)	movddup	a(b), c
#define MOVDDUP2(a, b, c)	movddup	a##b, c
#else
#define MOVDDUP(a, b, c)	movlpd	a(b), c;movhpd	a(b), c
#define MOVDDUP2(a, b, c)	movlpd	a##b, c;movhpd	a##b, c
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

	negq	IS
	addq	M, IS

	movq	IS,  TEMP
	imulq	LDA, TEMP
	addq	TEMP, A

	pcmpeqb	%xmm3,  %xmm3
	xorpd	%xmm2,  %xmm2
	pslld	$31,    %xmm3
	unpckhps %xmm3, %xmm2

	shufps	 $0, ALPHA_R, ALPHA_R
	shufps	 $0, ALPHA_I, ALPHA_I
	movaps	 ALPHA_I, %xmm3

	unpcklps ALPHA_R, ALPHA_I
	unpcklps %xmm3,   ALPHA_R
	pxor	 %xmm2,   ALPHA_R

	movq	BUFFER, XX

	movq	M,  %rax
	sarq	$2, %rax
	jle	.L02
	ALIGN_3

.L01:
	movsd	0 * SIZE(X), %xmm4
	addq	INCX, X
	movhps	0 * SIZE(X), %xmm4
	addq	INCX, X
	movsd	0 * SIZE(X), %xmm6
	addq	INCX, X
	movhps	0 * SIZE(X), %xmm6
	addq	INCX, X

	movsldup %xmm4, %xmm3
	movshdup %xmm4, %xmm4
	movsldup %xmm6, %xmm5
	movshdup %xmm6, %xmm6

	mulps	ALPHA_I, %xmm3
	mulps	ALPHA_R, %xmm4
	mulps	ALPHA_I, %xmm5
	mulps	ALPHA_R, %xmm6

	addps	%xmm4, %xmm3
	addps	%xmm6, %xmm5

	movaps	%xmm3,  4 * SIZE(XX)
	movaps	%xmm5, 12 * SIZE(XX)

	shufps	$0xb1, %xmm3, %xmm3
	shufps	$0xb1, %xmm5, %xmm5

	pxor	%xmm2, %xmm3
	pxor	%xmm2, %xmm5

	movaps	%xmm3,  0 * SIZE(XX)
	movaps	%xmm5,  8 * SIZE(XX)

	subq	$-16 * SIZE, XX
	decq	%rax
	jg	.L01
	ALIGN_3

.L02:
	testq	$2, M
	jle	.L03

	movsd	0 * SIZE(X), %xmm4
	addq	INCX, X
	movhps	0 * SIZE(X), %xmm4
	addq	INCX, X

	movsldup %xmm4, %xmm3
	movshdup %xmm4, %xmm4

	mulps	ALPHA_I, %xmm3
	mulps	ALPHA_R, %xmm4

	addps	%xmm4, %xmm3

	movaps	%xmm3,  4 * SIZE(XX)

	shufps	$0xb1, %xmm3, %xmm3
	pxor	%xmm2, %xmm3
	movaps	%xmm3,  0 * SIZE(XX)

	subq	$-8 * SIZE, XX
	ALIGN_3

.L03:
	testq	$1, M
	jle	.L05

	movsd	0 * SIZE(X), %xmm4
	addq	INCX, X

	movsldup %xmm4, %xmm3
	movshdup %xmm4, %xmm4

	mulps	ALPHA_I, %xmm3
	mulps	ALPHA_R, %xmm4

	addps	%xmm4, %xmm3

	movlps	%xmm3,  2 * SIZE(XX)

	shufps	$0xb1, %xmm3, %xmm3
	pxor	%xmm2, %xmm3
	movlps	%xmm3,  0 * SIZE(XX)

	subq	$-4 * SIZE, XX
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
	addq	INCY, YY
	movhps	0 * SIZE(YY), %xmm0
	addq	INCY, YY
	movsd	0 * SIZE(YY), %xmm1
	addq	INCY, YY
	movhps	0 * SIZE(YY), %xmm1
	addq	INCY, YY

	movaps	%xmm0, 0 * SIZE(XX)
	movaps	%xmm1, 8 * SIZE(XX)

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
	addq	INCY, YY

	movlps	%xmm0, 0 * SIZE(XX)

	addq	$2 * SIZE, XX
	decq	%rax
	jg	.L08
	ALIGN_3

.L10:
	movq	 IS, I
	addq	 $2, I
	cmpq	 M,  I
	jg	 .L20
	ALIGN_3

.L11:
	movq	A,  A1
	leaq	(A, LDA, 1), A2
	leaq	(A, LDA, 2), A

	leaq	(, IS, 4), I

	movsd	 0 * SIZE(NEW_X, I, SIZE), atemp2
	movhps	 4 * SIZE(NEW_X, I, SIZE), atemp2
	movsd	 2 * SIZE(NEW_X, I, SIZE), atemp4
	movhps	 6 * SIZE(NEW_X, I, SIZE), atemp4

	pshufd	 $0xcc, atemp2, atemp1
	pshufd	 $0x99, atemp2, atemp2
	pshufd	 $0xcc, atemp4, atemp3
	pshufd	 $0x99, atemp4, atemp4

	pxor		xsum1, xsum1
	pxor		xsum2, xsum2
	pxor		xsum3, xsum3
	pxor		xsum4, xsum4

	movq		NEW_X, XX
	movq		NEW_Y, YY

	movq	IS,  I
	sarq	$2,  I
	jle	.L15
	ALIGN_3

.L12:
	HALT

	subq	 $-16 * SIZE, XX
	addq	 $  8 * SIZE, YY
	addq	 $  8 * SIZE, A1
	addq	 $  8 * SIZE, A2

	decq	 I
	jg	 .L12
	ALIGN_3

.L15:
	testq	$2, IS
	jle	.L18

	movsd	 0 * SIZE(YY), yy1
	movhps	 2 * SIZE(YY), yy1

	movaps	 0 * SIZE(XX), xtemp1
	movaps	 4 * SIZE(XX), xtemp2

	movsd	 0 * SIZE(A1), a1
	movhps	 2 * SIZE(A1), a1

	movaps	 xtemp1, xt1
	movaps	 xtemp2, xt2
	mulps	 a1, xt1
	mulps	 a1, xt2
	addps	 xt1, xsum1
	addps	 xt2, xsum2

	pshufd	 $0xb1, a1, xt2
	mulps	 atemp1, a1
	mulps	 atemp2, xt2
	addps	 a1,  yy1
	addps	 xt2, yy1

	movsd	 0 * SIZE(A2), a1
	movhps	 2 * SIZE(A2), a1

	movaps	 xtemp1, xt1
	movaps	 xtemp2, xt2
	mulps	 a1, xt1
	mulps	 a1, xt2
	addps	 xt1, xsum3
	addps	 xt2, xsum4

	pshufd	 $0xb1, a1, xt2
	mulps	 atemp1, a1
	mulps	 atemp2, xt2
	addps	  a1, yy1
	addps	 xt2, yy1

	movlps	 yy1, 0 * SIZE(YY)
	movhps	 yy1, 2 * SIZE(YY)

	addq	 $8 * SIZE, XX
	addq	 $4 * SIZE, YY
	addq	 $4 * SIZE, A1
	addq	 $4 * SIZE, A2
	ALIGN_3

.L18:
	leaq	(, IS, 4), I

	movaps	 0 * SIZE(NEW_X, I, SIZE), atemp1
	movaps	 4 * SIZE(NEW_X, I, SIZE), atemp2

	movlps	 0 * SIZE(YY), yy1
	movhps	 2 * SIZE(YY), yy1

	movsd	 0 * SIZE(A1), a1
	movhps	 0 * SIZE(A2), a1

	movaps	 a1, a2
	mulps	 atemp1, a1
	mulps	 atemp2, a2
	addps	 a1, xsum1
	addps	 a2, xsum2

	movsd	 0 * SIZE(A2), a1
	movhps	 2 * SIZE(A2), a1

	movaps	 a1, a2
	mulps	 atemp1, a1
	mulps	 atemp2, a2
	addps	 a1, xsum3
	addps	 a2, xsum4

	haddps	 xsum2, xsum1
	haddps	 xsum4, xsum3

	haddps	 xsum3, xsum1
	addps	 xsum1, yy1

	movlps	 yy1, 0 * SIZE(YY)
	movhps	 yy1, 2 * SIZE(YY)

	addq	 $2, IS

	movq	 IS, I
	addq	 $2, I
	cmpq	 M, I
	jle	 .L11
	ALIGN_3

.L20:
	testq	$1, M
	jle	.L990


.L990:
	cmpq   $2 * SIZE, INCY
	je    .L999

	movq	M,  %rax
	sarq	$2, %rax
	jle	.L997
	ALIGN_3

.L996:
	movaps	 0 * SIZE(NEW_Y), %xmm0
	movaps	 4 * SIZE(NEW_Y), %xmm1

	movlps	%xmm0,  0 * SIZE(Y)
	addq	INCY, Y
	movhps	%xmm0,  0 * SIZE(Y)
	addq	INCY, Y
	movlps	%xmm1,  0 * SIZE(Y)
	addq	INCY, Y
	movhps	%xmm1,  0 * SIZE(Y)
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
	movlps	0 * SIZE(NEW_Y), %xmm0
	addq	$2 * SIZE, NEW_Y

	movlps	%xmm0,  0 * SIZE(Y)
	addq	INCY, Y

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
	addq	$STACKSIZE, %rsp
	ret
	EPILOGUE
