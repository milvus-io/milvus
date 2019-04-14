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

#if defined(BARCELONA) || defined(SHANGHAI) || defined(BULLDOZER)
#define RPREFETCHSIZE (12 + 4)
#define WPREFETCHSIZE (12 + 4)
#define MOVNTQ	 MOVQ
#else
#define RPREFETCHSIZE (12 + 4)
#define WPREFETCHSIZE (12 + 4)
#define MOVNTQ	 MOVQ
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
#define BO2	%r12
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
#define BO2	%rbx
#define M8	%rbp
#define BO	%rax

#endif

#if defined(BARCELONA) || defined(SHANGHAI) || defined(BULLDOZER)
#define RPREFETCH prefetch
#else
#define RPREFETCH prefetch
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
	movq	N,    %rbx
	andq	$-4,  %rax
	andq	$-2,  %rbx
	imulq	M,    %rax
	imulq	M,    %rbx

	EMMS

	leaq	(B, %rax, SIZE), BO1
	leaq	(B, %rbx, SIZE), BO2

	leaq	(,   LDA, SIZE), LDA
	leaq	(,   M,   SIZE), M8
	movq	M,  J
	sarq	$2, J
	jle	.L20
	ALIGN_4

.L11:
#if 0
	movq	A, AO1
	leaq	(A,   LDA, 1), AO2
	leaq	(A,   LDA, 2), AO3
	leaq	(AO2, LDA, 2), AO4

	movq	N,  I
	sarq	$3, I
	jle	.L13
	ALIGN_4

.L12:
	MOVQ	0 * SIZE(AO1), %mm0
	addq	$8 * SIZE, AO1
	MOVQ	0 * SIZE(AO2), %mm1
	addq	$8 * SIZE, AO2
	MOVQ	0 * SIZE(AO3), %mm2
	addq	$8 * SIZE, AO3
	MOVQ	0 * SIZE(AO4), %mm3
	addq	$8 * SIZE, AO4

	decq	I
	jg	.L12
	ALIGN_4

.L13:
#endif

	movq	A, AO1
	leaq	(A,   LDA   ), AO2
	leaq	(A,   LDA, 2), AO3
	leaq	(AO2, LDA, 2), AO4
	leaq	(A,   LDA, 4), A

	movq	B, BO
	addq	$16 * SIZE, B

	movq	N,  I
	sarq	$2, I
	jle	.L15
	ALIGN_4

.L14:

	RPREFETCH	(RPREFETCHSIZE) * SIZE(AO1)

	MOVQ	0 * SIZE(AO1), %mm0
	MOVNTQ	%mm0,   0 * SIZE(BO)
	MOVQ	1 * SIZE(AO1), %mm1
	MOVNTQ	%mm1,   1 * SIZE(BO)

	RPREFETCH	(RPREFETCHSIZE) * SIZE(AO2)

	MOVQ	2 * SIZE(AO1), %mm2
	MOVNTQ	%mm2,   2 * SIZE(BO)
	MOVQ	3 * SIZE(AO1), %mm3
	MOVNTQ	%mm3,   3 * SIZE(BO)

	prefetchw	(WPREFETCHSIZE +  0) * SIZE(B)
	MOVQ	0 * SIZE(AO2), %mm4
	MOVNTQ	%mm4,   4 * SIZE(BO)
	MOVQ	1 * SIZE(AO2), %mm5
	MOVNTQ	%mm5,   5 * SIZE(BO)
	MOVQ	2 * SIZE(AO2), %mm6
	MOVNTQ	%mm6,   6 * SIZE(BO)
	MOVQ	3 * SIZE(AO2), %mm7
	MOVNTQ	%mm7,   7 * SIZE(BO)


	RPREFETCH	(RPREFETCHSIZE) * SIZE(AO3)

	MOVQ	0 * SIZE(AO3), %mm0
	MOVNTQ	%mm0,   8 * SIZE(BO)
	MOVQ	1 * SIZE(AO3), %mm1
	MOVNTQ	%mm1,   9 * SIZE(BO)

	RPREFETCH	(RPREFETCHSIZE) * SIZE(AO4)

	MOVQ	2 * SIZE(AO3), %mm2
 	MOVNTQ	%mm2,  10 * SIZE(BO)
	MOVQ	3 * SIZE(AO3), %mm3
 	MOVNTQ	%mm3,  11 * SIZE(BO)

	prefetchw	(WPREFETCHSIZE +  8) * SIZE(B)
	MOVQ	0 * SIZE(AO4), %mm4
	MOVNTQ	%mm4,  12 * SIZE(BO)
	MOVQ	1 * SIZE(AO4), %mm5
	MOVNTQ	%mm5,  13 * SIZE(BO)
	MOVQ	2 * SIZE(AO4), %mm6
	MOVNTQ	%mm6,  14 * SIZE(BO)
	MOVQ	3 * SIZE(AO4), %mm7
	MOVNTQ	%mm7,  15 * SIZE(BO)

	addq	$4 * SIZE, AO1
	addq	$4 * SIZE, AO2
	addq	$4 * SIZE, AO3
	addq	$4 * SIZE, AO4

	leaq	(BO, M8, 4), BO
	decq	I
	jg	.L14
	ALIGN_4

.L15:
	testq	$2, N
	jle	.L16

	MOVQ	0 * SIZE(AO1), %mm0
	MOVQ	1 * SIZE(AO1), %mm1
	MOVQ	0 * SIZE(AO2), %mm2
	MOVQ	1 * SIZE(AO2), %mm3

	MOVQ	0 * SIZE(AO3), %mm4
	MOVQ	1 * SIZE(AO3), %mm5
	MOVQ	0 * SIZE(AO4), %mm6
	MOVQ	1 * SIZE(AO4), %mm7

	MOVNTQ	%mm0,   0 * SIZE(BO1)
	MOVNTQ	%mm1,   1 * SIZE(BO1)
	MOVNTQ	%mm2,   2 * SIZE(BO1)
	MOVNTQ	%mm3,   3 * SIZE(BO1)
	MOVNTQ	%mm4,   4 * SIZE(BO1)
	MOVNTQ	%mm5,   5 * SIZE(BO1)
	MOVNTQ	%mm6,   6 * SIZE(BO1)
	MOVNTQ	%mm7,   7 * SIZE(BO1)

	addq	$2 * SIZE, AO1
	addq	$2 * SIZE, AO2
	addq	$2 * SIZE, AO3
	addq	$2 * SIZE, AO4
	addq	$8 * SIZE, BO1
	ALIGN_4

.L16:
	testq	$1, N
	jle	.L19

	MOVQ	0 * SIZE(AO1), %mm0
	MOVQ	0 * SIZE(AO2), %mm1
	MOVQ	0 * SIZE(AO3), %mm2
	MOVQ	0 * SIZE(AO4), %mm3

	MOVNTQ	%mm0,   0 * SIZE(BO2)
	MOVNTQ	%mm1,   1 * SIZE(BO2)
	MOVNTQ	%mm2,   2 * SIZE(BO2)
	MOVNTQ	%mm3,   3 * SIZE(BO2)

	addq	$4 * SIZE, BO2
	ALIGN_4

.L19:
	decq	J
	jg	.L11
	ALIGN_4

.L20:
	testq	$2, M
	jle	.L30
	ALIGN_4

.L21:
	movq	A, AO1
	leaq	(A,   LDA   ), AO2
	leaq	(A,   LDA, 2), A

	movq	B, BO
	addq	$8 * SIZE, B

	movq	N,  I
	sarq	$2, I
	jle	.L23
	ALIGN_4

.L22:
	RPREFETCH	(RPREFETCHSIZE) * SIZE(AO1)
	MOVQ	0 * SIZE(AO1), %mm0
	MOVQ	1 * SIZE(AO1), %mm1
	MOVQ	2 * SIZE(AO1), %mm2
	MOVQ	3 * SIZE(AO1), %mm3

	RPREFETCH	(RPREFETCHSIZE) * SIZE(AO2)
	MOVQ	0 * SIZE(AO2), %mm4
	MOVQ	1 * SIZE(AO2), %mm5
	MOVQ	2 * SIZE(AO2), %mm6
	MOVQ	3 * SIZE(AO2), %mm7

	prefetchw	(WPREFETCHSIZE +  0) * SIZE(B)
	MOVNTQ	%mm0,   0 * SIZE(BO)
	MOVNTQ	%mm1,   1 * SIZE(BO)
	MOVNTQ	%mm2,   2 * SIZE(BO)
	MOVNTQ	%mm3,   3 * SIZE(BO)
	MOVNTQ	%mm4,   4 * SIZE(BO)
	MOVNTQ	%mm5,   5 * SIZE(BO)
	MOVNTQ	%mm6,   6 * SIZE(BO)
	MOVNTQ	%mm7,   7 * SIZE(BO)

	addq	$4 * SIZE, AO1
	addq	$4 * SIZE, AO2
	leaq	(BO, M8, 4), BO
	decq	I
	jg	.L22
	ALIGN_4

.L23:
	testq	$2, N
	jle	.L24

	MOVQ	0 * SIZE(AO1), %mm0
	MOVQ	1 * SIZE(AO1), %mm1
	MOVQ	0 * SIZE(AO2), %mm2
	MOVQ	1 * SIZE(AO2), %mm3

	MOVNTQ	%mm0,   0 * SIZE(BO1)
	MOVNTQ	%mm1,   1 * SIZE(BO1)
	MOVNTQ	%mm2,   2 * SIZE(BO1)
	MOVNTQ	%mm3,   3 * SIZE(BO1)

	addq	$2 * SIZE, AO1
	addq	$2 * SIZE, AO2
	addq	$4 * SIZE, BO1
	ALIGN_4

.L24:
	testq	$1, N
	jle	.L30

	MOVQ	0 * SIZE(AO1), %mm0
	MOVQ	0 * SIZE(AO2), %mm1

	MOVNTQ	%mm0,   0 * SIZE(BO2)
	MOVNTQ	%mm1,   1 * SIZE(BO2)

	addq	$2 * SIZE, BO2
	ALIGN_4

.L30:
	testq	$1, M
	jle	.L999
	ALIGN_4

.L31:
	movq	A, AO1
	movq	B, BO

	movq	N,  I
	sarq	$2, I
	jle	.L33
	ALIGN_4

.L32:
	MOVQ	0 * SIZE(AO1), %mm0
	MOVQ	1 * SIZE(AO1), %mm1
	MOVQ	2 * SIZE(AO1), %mm2
	MOVQ	3 * SIZE(AO1), %mm3

	MOVNTQ	%mm0,   0 * SIZE(BO)
	MOVNTQ	%mm1,   1 * SIZE(BO)
	MOVNTQ	%mm2,   2 * SIZE(BO)
	MOVNTQ	%mm3,   3 * SIZE(BO)

	addq	$4 * SIZE, AO1
	leaq	(BO, M8, 4), BO
	decq	I
	jg	.L32
	ALIGN_4

.L33:
	testq	$2, N
	jle	.L34

	MOVQ	0 * SIZE(AO1), %mm0
	MOVQ	1 * SIZE(AO1), %mm1

	MOVNTQ	%mm0,   0 * SIZE(BO1)
	MOVNTQ	%mm1,   1 * SIZE(BO1)

	addq	$2 * SIZE, AO1
	addq	$2 * SIZE, BO1
	ALIGN_4

.L34:
	testq	$1, N
	jle	.L999

	MOVQ	0 * SIZE(AO1), %mm0
	MOVNTQ	%mm0,   0 * SIZE(BO2)

	addq	$1 * SIZE, BO2
	ALIGN_4

.L999:
	EMMS

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
