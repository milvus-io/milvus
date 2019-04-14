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

#if defined(NEHALEM)
#define RPREFETCHSIZE	12
#define WPREFETCHSIZE (RPREFETCHSIZE * 2)
#define PREFETCH      prefetcht0
#define PREFETCHW     prefetcht0
#endif

#if defined(SANDYBRIDGE)
#define RPREFETCHSIZE	12
#define WPREFETCHSIZE (RPREFETCHSIZE * 2)
#define PREFETCH      prefetcht0
#define PREFETCHW     prefetcht0
#endif

#ifndef WINDOWS_ABI

#define M	ARG1	/* rdi */
#define N	ARG2	/* rsi */
#define A	ARG3	/* rdx */
#define LDA	ARG4	/* rcx */
#define B	ARG5	/* r8  */

#define I	%r10
#define J	%rbp

#define AO1	%r9
#define AO2	%r15
#define AO3	%r11
#define AO4	%r14
#define BO1	%r13
#define M8	%rbx
#define BO	%rax

#else

#define STACKSIZE 256

#define M	ARG1	/* rcx */
#define N	ARG2	/* rdx */
#define A	ARG3	/* r8  */
#define LDA	ARG4	/* r9  */
#define OLD_B		40 + 64 + STACKSIZE(%rsp)

#define B	%rdi

#define I	%r10
#define J	%r11

#define AO1	%r12
#define AO2	%r13
#define AO3	%r14
#define AO4	%r15

#define BO1	%rsi
#define M8	%rbp
#define BO	%rax

#endif

	PROLOGUE
	PROFCODE

#ifdef WINDOWS_ABI
	pushq	%rdi
	pushq	%rsi
#endif
	pushq	%r15
	pushq	%r14
	pushq	%r13
	pushq	%r12
	pushq	%rbp
	pushq	%rbx

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

	leaq	(B, %rax, SIZE), BO1

	leaq	(,   LDA, SIZE), LDA
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
	addq	$4 * SIZE, B

	movq	N,  I
	sarq	$1, I
	jle	.L14
	ALIGN_4

.L12:
#ifndef DOUBLE
	movlps	0 * SIZE(AO1), %xmm0
	movhps	0 * SIZE(AO2), %xmm0

	movaps	%xmm0,    0 * SIZE(BO)
#else
	PREFETCH	RPREFETCHSIZE * SIZE(AO1)
	movsd	0 * SIZE(AO1), %xmm0
	movhpd	1 * SIZE(AO1), %xmm0
	PREFETCH	RPREFETCHSIZE * SIZE(AO2)
	movsd	0 * SIZE(AO2), %xmm1
	movhpd	1 * SIZE(AO2), %xmm1

	PREFETCHW	WPREFETCHSIZE * SIZE(BO)
	movapd	%xmm0,    0 * SIZE(BO)
	movapd	%xmm1,    2 * SIZE(BO)
#endif

	leaq	(BO, M8, 2), BO
	addq	$2 * SIZE, AO1
	addq	$2 * SIZE, AO2
	decq	I
	jg	.L12
	ALIGN_4

.L14:
	testq	$1, N
	jle	.L19

#ifndef DOUBLE
	movss	0 * SIZE(AO1), %xmm0
	movss	0 * SIZE(AO2), %xmm1

	movss	%xmm0,    0 * SIZE(BO1)
	movss	%xmm1,    1 * SIZE(BO1)
#else
	movsd	0 * SIZE(AO1), %xmm0
	movhpd	0 * SIZE(AO2), %xmm0

	movapd	%xmm0,    0 * SIZE(BO1)
#endif

	addq	$2 * SIZE, BO1
	ALIGN_4

.L19:
	decq	J
	jg	.L11
	ALIGN_4

.L20:
	testq	$1, M
	jle	.L999
	ALIGN_4

.L31:
	movq	A, AO1
	movq	B, BO

	movq	N,  I
	sarq	$1, I
	jle	.L33
	ALIGN_4

.L32:
#ifndef DOUBLE
	movsd	0 * SIZE(AO1), %xmm0
	movsd	%xmm0,    0 * SIZE(BO)
#else
	movsd	0 * SIZE(AO1), %xmm0
	movhpd	1 * SIZE(AO1), %xmm0
	movapd	%xmm0,    0 * SIZE(BO)
#endif

	addq	$2 * SIZE, AO1
	leaq	(BO, M8, 2), BO
	decq	I
	jg	.L32
	ALIGN_4

.L33:
	testq	$1, N
	jle	.L999

#ifndef DOUBLE
	movss	0 * SIZE(AO1), %xmm0
	movss	%xmm0,    0 * SIZE(BO1)
#else
	movsd	0 * SIZE(AO1), %xmm0
	movsd	%xmm0,    0 * SIZE(BO1)
#endif
	addq	$1 * SIZE, BO1
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

	popq	%rbx
	popq	%rbp
	popq	%r12
	popq	%r13
	popq	%r14
	popq	%r15
#ifdef WINDOWS_ABI
	popq	%rsi
	popq	%rdi
#endif

	ret

	EPILOGUE
