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

	vmovups	%xmm6,    0(%rsp)
	vmovups	%xmm7,   16(%rsp)
	vmovups	%xmm8,   32(%rsp)
	vmovups	%xmm9,   48(%rsp)
	vmovups	%xmm10,  64(%rsp)
	vmovups	%xmm11,  80(%rsp)
	vmovups	%xmm12,  96(%rsp)
	vmovups	%xmm13, 112(%rsp)
	vmovups	%xmm14, 128(%rsp)
	vmovups	%xmm15, 144(%rsp)

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

.L01:
	movq	A, AO1
	leaq	(A,   LDA   ), AO2
	leaq	(A,   LDA, 2), A

	movq	B, BO
	addq	$4 * SIZE, B

	movq	N,  I
	sarq	$3, I
	jle	.L10
	ALIGN_4


.L08:
#ifndef DOUBLE

	vmovsd	0 * SIZE(AO1), %xmm0
	vmovsd	2 * SIZE(AO1), %xmm2
	vmovsd	4 * SIZE(AO1), %xmm4
	vmovsd	6 * SIZE(AO1), %xmm6
	vmovsd	0 * SIZE(AO2), %xmm1
	vmovsd	2 * SIZE(AO2), %xmm3
	vmovsd	4 * SIZE(AO2), %xmm5
	vmovsd	6 * SIZE(AO2), %xmm7

	vmovsd	%xmm0,    0 * SIZE(BO)
	vmovsd	%xmm1,    2 * SIZE(BO)
	leaq	(BO, M8, 2), BO

	vmovsd	%xmm2,    0 * SIZE(BO)
	vmovsd	%xmm3,    2 * SIZE(BO)
	leaq	(BO, M8, 2), BO

	vmovsd	%xmm4,    0 * SIZE(BO)
	vmovsd	%xmm5,    2 * SIZE(BO)
	leaq	(BO, M8, 2), BO

	vmovsd	%xmm6,    0 * SIZE(BO)
	vmovsd	%xmm7,    2 * SIZE(BO)
	leaq	(BO, M8, 2), BO


#else

	prefetchnta	256(AO1)
	prefetchnta	256(AO2)
	vmovups	0 * SIZE(AO1), %xmm0
	vmovups	2 * SIZE(AO1), %xmm2
	vmovups	4 * SIZE(AO1), %xmm4
	vmovups	6 * SIZE(AO1), %xmm6
	vmovups	0 * SIZE(AO2), %xmm1
	vmovups	2 * SIZE(AO2), %xmm3
	vmovups	4 * SIZE(AO2), %xmm5
	vmovups	6 * SIZE(AO2), %xmm7

	vmovups	%xmm0,    0 * SIZE(BO)
	vmovups	%xmm1,    2 * SIZE(BO)
	leaq	(BO, M8, 2), BO

	vmovups	%xmm2,    0 * SIZE(BO)
	vmovups	%xmm3,    2 * SIZE(BO)
	leaq	(BO, M8, 2), BO

	vmovups	%xmm4,    0 * SIZE(BO)
	vmovups	%xmm5,    2 * SIZE(BO)
	leaq	(BO, M8, 2), BO

	vmovups	%xmm6,    0 * SIZE(BO)
	vmovups	%xmm7,    2 * SIZE(BO)
	leaq	(BO, M8, 2), BO

#endif

	addq	$8 * SIZE, AO1
	addq	$8 * SIZE, AO2
	decq	I
	jg	.L08
	ALIGN_4



.L10:
	testq	$4, N
	jle	.L12
#ifndef DOUBLE

	vmovsd	0 * SIZE(AO1), %xmm0
	vmovsd	2 * SIZE(AO1), %xmm2
	vmovsd	0 * SIZE(AO2), %xmm1
	vmovsd	2 * SIZE(AO2), %xmm3

	vmovsd	%xmm0,    0 * SIZE(BO)
	vmovsd	%xmm1,    2 * SIZE(BO)
	leaq	(BO, M8, 2), BO

	vmovsd	%xmm2,    0 * SIZE(BO)
	vmovsd	%xmm3,    2 * SIZE(BO)
	leaq	(BO, M8, 2), BO


#else

	vmovups	0 * SIZE(AO1), %xmm0
	vmovups	2 * SIZE(AO1), %xmm2
	vmovups	0 * SIZE(AO2), %xmm1
	vmovups	2 * SIZE(AO2), %xmm3

	vmovups	%xmm0,    0 * SIZE(BO)
	vmovups	%xmm1,    2 * SIZE(BO)
	leaq	(BO, M8, 2), BO

	vmovups	%xmm2,    0 * SIZE(BO)
	vmovups	%xmm3,    2 * SIZE(BO)
	leaq	(BO, M8, 2), BO

#endif

	addq	$4 * SIZE, AO1
	addq	$4 * SIZE, AO2
	ALIGN_4


.L12:
	testq	$2, N
	jle	.L14
#ifndef DOUBLE
	vmovsd	0 * SIZE(AO1), %xmm0
	vmovsd	0 * SIZE(AO2), %xmm1

	vmovsd	%xmm0,    0 * SIZE(BO)
	vmovsd	%xmm1,    2 * SIZE(BO)
#else
	vmovups	0 * SIZE(AO1), %xmm0
	vmovups	0 * SIZE(AO2), %xmm1

	vmovups	%xmm0,    0 * SIZE(BO)
	vmovups	%xmm1,    2 * SIZE(BO)
#endif

	leaq	(BO, M8, 2), BO
	addq	$2 * SIZE, AO1
	addq	$2 * SIZE, AO2
	ALIGN_4

.L14:
	testq	$1, N
	jle	.L19

#ifndef DOUBLE
	vmovss	0 * SIZE(AO1), %xmm0
	vmovss	0 * SIZE(AO2), %xmm1

	vmovss	%xmm0,    0 * SIZE(BO1)
	vmovss	%xmm1,    1 * SIZE(BO1)
#else
	vmovsd	0 * SIZE(AO1), %xmm0
	vmovhpd	0 * SIZE(AO2), %xmm0 , %xmm0

	vmovups	%xmm0,    0 * SIZE(BO1)
#endif

	addq	$2 * SIZE, BO1
	ALIGN_4

.L19:
	decq	J
	jg	.L01
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
	vmovsd	0 * SIZE(AO1), %xmm0
	vmovsd	%xmm0,    0 * SIZE(BO)
#else
	vmovups	0 * SIZE(AO1), %xmm0
	vmovups	%xmm0,    0 * SIZE(BO)
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
	vmovss	0 * SIZE(AO1), %xmm0
	vmovss	%xmm0,    0 * SIZE(BO1)
#else
	vmovsd	0 * SIZE(AO1), %xmm0
	vmovsd	%xmm0,    0 * SIZE(BO1)
#endif
	addq	$1 * SIZE, BO1
	ALIGN_4

.L999:
#ifdef WINDOWS_ABI
	vmovups	  0(%rsp), %xmm6
	vmovups	 16(%rsp), %xmm7
	vmovups	 32(%rsp), %xmm8
	vmovups	 48(%rsp), %xmm9
	vmovups	 64(%rsp), %xmm10
	vmovups	 80(%rsp), %xmm11
	vmovups	 96(%rsp), %xmm12
	vmovups	112(%rsp), %xmm13
	vmovups	128(%rsp), %xmm14
	vmovups	144(%rsp), %xmm15

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
