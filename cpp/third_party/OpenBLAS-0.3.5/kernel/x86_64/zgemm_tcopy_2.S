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

#define RPREFETCHSIZE 32
#define WPREFETCHSIZE 16

#ifndef WINDOWS_ABI

#define M	ARG1	/* rdi */
#define N	ARG2	/* rsi */
#define A	ARG3	/* rdx */
#define LDA	ARG4	/* rcx */
#define B	ARG5	/* r8  */

#define I	%r9
#define J	%r10

#define AO1	%r11
#define AO2	%r12
#define BO1	%r13
#define M8	%r14
#define BO	%rax

#else

#define STACKSIZE 256

#define M	ARG1	/* rcx */
#define N	ARG2	/* rdx */
#define A	ARG3	/* r8  */
#define LDA	ARG4	/* r9  */
#define OLD_B		40 + 48 + STACKSIZE(%rsp)

#define B	%r10

#define I	%r11
#define J	%r12

#define AO1	%r13
#define AO2	%r14

#define BO1	%rdi
#define M8	%rsi
#define BO	%rax

#endif

	PROLOGUE
	PROFCODE

#ifdef WINDOWS_ABI
	pushq	%rdi
	pushq	%rsi
#endif
	pushq	%r14
	pushq	%r13
	pushq	%r12
	pushq	%r11

#ifdef WINDOWS_ABI
	subq	$STACKSIZE, %rsp

	movups	%xmm6,    0(%rsp)
	movups	%xmm7,   16(%rsp)
	movups	%xmm8,   32(%rsp)
	movups	%xmm9,   48(%rsp)
	movups	%xmm10,  64(%rsp)
	movups	%xmm11,  80(%rsp)
	movups	%xmm12,  96(%rsp)
	movups	%xmm13, 112(%rsp)
	movups	%xmm14, 128(%rsp)
	movups	%xmm15, 144(%rsp)

	movq	OLD_B,     B
#endif

	movq	N,    %rax
	andq	$-2,  %rax
	imulq	M,    %rax

	salq	$ZBASE_SHIFT, %rax
	leaq	(B, %rax, 1), BO1

	salq	$ZBASE_SHIFT, LDA

	leaq	(,   M,   SIZE), M8

	movq	M,  J
	sarq	$1, J
	jle	.L20
	ALIGN_4

.L11:
	movq	A, AO1
	leaq	(A,   LDA   ), AO2
	leaq	(A,   LDA, 2), A

	movq	B, BO
	addq	$8 * SIZE, B

	movq	N,  I
	sarq	$2, I
	jle	.L13
	ALIGN_4

.L12:
#ifndef DOUBLE
	movlps	0 * SIZE(AO1), %xmm0
	movhps	2 * SIZE(AO1), %xmm0
	movlps	4 * SIZE(AO1), %xmm1
	movhps	6 * SIZE(AO1), %xmm1

	movlps	0 * SIZE(AO2), %xmm2
	movhps	2 * SIZE(AO2), %xmm2
	movlps	4 * SIZE(AO2), %xmm3
	movhps	6 * SIZE(AO2), %xmm3

#if defined(PENTIUM4) || defined(GENERIC) || defined(NANO)
	prefetcht0	RPREFETCHSIZE * SIZE(AO1)
	prefetcht0	RPREFETCHSIZE * SIZE(AO2)
	prefetcht0	WPREFETCHSIZE * SIZE(BO)
#endif

#ifdef HAVE_3DNOW
	prefetchw	(WPREFETCHSIZE + 0) * SIZE(BO)
#endif

	movaps	%xmm0,    0 * SIZE(BO)
	movaps	%xmm2,    4 * SIZE(BO)
	leaq	(BO, M8, 4), BO
	movaps	%xmm1,    0 * SIZE(BO)
	movaps	%xmm3,    4 * SIZE(BO)
#else
	movsd	0 * SIZE(AO1), %xmm0
	movhpd	1 * SIZE(AO1), %xmm0
	movsd	2 * SIZE(AO1), %xmm1
	movhpd	3 * SIZE(AO1), %xmm1

#if defined(CORE2) || defined(PENRYN) || defined(DUNNINGTON)
	prefetcht2	RPREFETCHSIZE * SIZE(AO1)
#endif

	movsd	4 * SIZE(AO1), %xmm2
	movhpd	5 * SIZE(AO1), %xmm2
	movsd	6 * SIZE(AO1), %xmm3
	movhpd	7 * SIZE(AO1), %xmm3

	movsd	0 * SIZE(AO2), %xmm4
	movhpd	1 * SIZE(AO2), %xmm4
	movsd	2 * SIZE(AO2), %xmm5
	movhpd	3 * SIZE(AO2), %xmm5

#if defined(CORE2) || defined(PENRYN) || defined(DUNNINGTON)
	prefetcht2	RPREFETCHSIZE * SIZE(AO2)
#endif

	movsd	4 * SIZE(AO2), %xmm6
	movhpd	5 * SIZE(AO2), %xmm6
	movsd	6 * SIZE(AO2), %xmm7
	movhpd	7 * SIZE(AO2), %xmm7

#if defined(PENTIUM4) || defined(GENERIC) || defined(NANO)
	prefetcht0	RPREFETCHSIZE * SIZE(AO1)
	prefetcht0	RPREFETCHSIZE * SIZE(AO2)
	prefetcht0	WPREFETCHSIZE * SIZE(BO)
#endif

#ifdef HAVE_3DNOW
	prefetchw	(WPREFETCHSIZE + 0) * SIZE(BO)
	prefetchw	(WPREFETCHSIZE + 8) * SIZE(BO)
#endif

	movapd	%xmm0,    0 * SIZE(BO)
	movapd	%xmm1,    2 * SIZE(BO)
	movapd	%xmm4,    4 * SIZE(BO)
	movapd	%xmm5,    6 * SIZE(BO)

#if defined(CORE2) || defined(PENRYN) || defined(DUNNINGTON)
	prefetcht2	WPREFETCHSIZE * SIZE(BO)
#endif
	leaq	(BO, M8, 4), BO

	movapd	%xmm2,    0 * SIZE(BO)
	movapd	%xmm3,    2 * SIZE(BO)
	movapd	%xmm6,    4 * SIZE(BO)
	movapd	%xmm7,    6 * SIZE(BO)
#endif

	addq	$8 * SIZE, AO1
	addq	$8 * SIZE, AO2
	leaq	(BO, M8, 4), BO
	decq	I
	jg	.L12
	ALIGN_4

.L13:
	testq	$2, N
	jle	.L14

#ifndef DOUBLE
	movlps	0 * SIZE(AO1), %xmm0
	movhps	2 * SIZE(AO1), %xmm0

	movlps	0 * SIZE(AO2), %xmm1
	movhps	2 * SIZE(AO2), %xmm1

	movaps	%xmm0,    0 * SIZE(BO)
	movaps	%xmm1,    4 * SIZE(BO)
#else
	movsd	0 * SIZE(AO1), %xmm0
	movhpd	1 * SIZE(AO1), %xmm0
	movsd	2 * SIZE(AO1), %xmm1
	movhpd	3 * SIZE(AO1), %xmm1

	movsd	0 * SIZE(AO2), %xmm2
	movhpd	1 * SIZE(AO2), %xmm2
	movsd	2 * SIZE(AO2), %xmm3
	movhpd	3 * SIZE(AO2), %xmm3

	movapd	%xmm0,    0 * SIZE(BO)
	movapd	%xmm1,    2 * SIZE(BO)
	movapd	%xmm2,    4 * SIZE(BO)
	movapd	%xmm3,    6 * SIZE(BO)
#endif

	addq	$4 * SIZE, AO1
	addq	$4 * SIZE, AO2
	leaq	(BO, M8, 4), BO
	ALIGN_4

.L14:
	testq	$1, N
	jle	.L19

#ifndef DOUBLE
	movlps	0 * SIZE(AO1), %xmm0
	movhps	0 * SIZE(AO2), %xmm0

	movaps	%xmm0,    0 * SIZE(BO1)
#else
	movsd	0 * SIZE(AO1), %xmm0
	movhpd	1 * SIZE(AO1), %xmm0
	movsd	0 * SIZE(AO2), %xmm1
	movhpd	1 * SIZE(AO2), %xmm1

	movapd	%xmm0,    0 * SIZE(BO1)
	movapd	%xmm1,    2 * SIZE(BO1)
#endif

	addq	$4 * SIZE, BO1
	ALIGN_4

.L19:
	decq	J
	jg	.L11
	ALIGN_4

.L20:
	testq	$1, M
	jle	.L999
	ALIGN_4

.L21:
	movq	A, AO1

	movq	B, BO

	movq	N,  I
	sarq	$2, I
	jle	.L23
	ALIGN_4

.L22:
#ifndef DOUBLE
	movlps	0 * SIZE(AO1), %xmm0
	movhps	2 * SIZE(AO1), %xmm0

	movlps	4 * SIZE(AO1), %xmm1
	movhps	6 * SIZE(AO1), %xmm1

#if defined(PENTIUM4) || defined(GENERIC) || defined(NANO)
	prefetcht0	RPREFETCHSIZE * SIZE(AO1)
	prefetcht0	WPREFETCHSIZE * SIZE(BO)
#endif

#ifdef HAVE_3DNOW
	prefetchw	(WPREFETCHSIZE + 0) * SIZE(BO)
#endif

	movaps	%xmm0,    0 * SIZE(BO)
	leaq	(BO, M8, 4), BO
	movaps	%xmm1,    0 * SIZE(BO)
#else
	movsd	0 * SIZE(AO1), %xmm0
	movhpd	1 * SIZE(AO1), %xmm0
	movsd	2 * SIZE(AO1), %xmm1
	movhpd	3 * SIZE(AO1), %xmm1

	movsd	4 * SIZE(AO1), %xmm2
	movhpd	5 * SIZE(AO1), %xmm2
	movsd	6 * SIZE(AO1), %xmm3
	movhpd	7 * SIZE(AO1), %xmm3

#if defined(PENTIUM4) || defined(GENERIC) || defined(NANO)
	prefetcht0	RPREFETCHSIZE * SIZE(AO1)
	prefetcht0	WPREFETCHSIZE * SIZE(BO)
#endif

#ifdef HAVE_3DNOW
	prefetchw	(WPREFETCHSIZE + 0) * SIZE(BO)
#endif

	movapd	%xmm0,    0 * SIZE(BO)
	movapd	%xmm1,    2 * SIZE(BO)

	leaq	(BO, M8, 4), BO

	movapd	%xmm2,    0 * SIZE(BO)
	movapd	%xmm3,    2 * SIZE(BO)
#endif

	addq	$8 * SIZE, AO1
	leaq	(BO, M8, 4), BO
	decq	I
	jg	.L22
	ALIGN_4

.L23:
	testq	$2, N
	jle	.L24

#ifndef DOUBLE
	movlps	0 * SIZE(AO1), %xmm0
	movhps	2 * SIZE(AO1), %xmm0

	movaps	%xmm0,    0 * SIZE(BO)
#else
	movsd	0 * SIZE(AO1), %xmm0
	movhpd	1 * SIZE(AO1), %xmm0
	movsd	2 * SIZE(AO1), %xmm1
	movhpd	3 * SIZE(AO1), %xmm1

	movapd	%xmm0,    0 * SIZE(BO)
	movapd	%xmm1,    2 * SIZE(BO)
#endif

	addq	$4 * SIZE, AO1
	leaq	(BO, M8, 4), BO
	ALIGN_4

.L24:
	testq	$1, N
	jle	.L999

#ifndef DOUBLE
	movlps	0 * SIZE(AO1), %xmm0

	movlps	%xmm0,    0 * SIZE(BO1)
#else
	movsd	0 * SIZE(AO1), %xmm0
	movhpd	1 * SIZE(AO1), %xmm0

	movapd	%xmm0,    0 * SIZE(BO1)
#endif
	ALIGN_4


.L999:
#ifdef WINDOWS_ABI
	movups	  0(%rsp), %xmm6
	movups	 16(%rsp), %xmm7
	movups	 32(%rsp), %xmm8
	movups	 48(%rsp), %xmm9
	movups	 64(%rsp), %xmm10
	movups	 80(%rsp), %xmm11
	movups	 96(%rsp), %xmm12
	movups	112(%rsp), %xmm13
	movups	128(%rsp), %xmm14
	movups	144(%rsp), %xmm15

	addq	$STACKSIZE, %rsp
#endif

	popq	%r11
	popq	%r12
	popq	%r13
	popq	%r14
#ifdef WINDOWS_ABI
	popq	%rsi
	popq	%rdi
#endif
	ret

	EPILOGUE
