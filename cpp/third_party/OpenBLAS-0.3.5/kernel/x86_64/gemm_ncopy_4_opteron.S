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
#define WPREFETCHSIZE (48 + 4)
#define MOVNTQ	 MOVQ
#else
#define RPREFETCHSIZE (12 + 4)
#define WPREFETCHSIZE (24 + 4)
#define MOVNTQ	 MOVQ
#endif

#ifndef WINDOWS_ABI

#define M	ARG1	/* rdi */
#define N	ARG2	/* rsi */
#define A	ARG3	/* rdx */
#define LDA	ARG4	/* rcx */
#define B	ARG5	/* r8  */

#define I	%r9
#else

#define STACKSIZE 256

#define M	ARG1	/* rcx */
#define N	ARG2	/* rdx */
#define A	ARG3	/* r8  */
#define LDA	ARG4	/* r9  */
#define OLD_B		40 + 32 + STACKSIZE(%rsp)

#define B	%r14
#define I	%r15

#endif

#define J	%r10
#define AO1	%r11
#define AO2	%r12
#define AO3	%r13
#define AO4	%rax

#if defined(BARCELONA) || defined(SHANGHAI) || defined(BULLDOZER)
#define RPREFETCH prefetch
#else
#define RPREFETCH prefetch
#endif

	PROLOGUE
	PROFCODE

#ifdef WINDOWS_ABI
	pushq	%r15
	pushq	%r14
#endif
	pushq	%r13
	pushq	%r12

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

	EMMS

	leaq	(,LDA, SIZE), LDA		# Scaling

	movq	N,  J
	sarq	$2, J
	jle	.L20
	ALIGN_4

.L11:
#if 0
	movq	A, AO1
	leaq	(A,   LDA, 1), AO2
	leaq	(A,   LDA, 2), AO3
	leaq	(AO2, LDA, 2), AO4

	movq	M,  I
	sarq	$4, I
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
	leaq	(A,   LDA), AO2
	leaq	(A,   LDA, 2), AO3
	leaq	(AO2, LDA, 2), AO4
	leaq	(A, LDA, 4), A

	movq	M,  I
	sarq	$2, I
	jle	.L15
	ALIGN_4

.L14:
	RPREFETCH	(RPREFETCHSIZE) * SIZE(AO1)

	MOVQ	0 * SIZE(AO1), %mm0
	MOVNTQ	%mm0,   0 * SIZE(B)
	MOVQ	0 * SIZE(AO2), %mm1
	MOVNTQ	%mm1,   1 * SIZE(B)

	RPREFETCH	(RPREFETCHSIZE) * SIZE(AO2)

	MOVQ	0 * SIZE(AO3), %mm2
	MOVNTQ	%mm2,   2 * SIZE(B)
	MOVQ	0 * SIZE(AO4), %mm3
	MOVNTQ	%mm3,   3 * SIZE(B)

	prefetchw	(WPREFETCHSIZE +  0) * SIZE(B)
	MOVQ	1 * SIZE(AO1), %mm4
	MOVNTQ	%mm4,   4 * SIZE(B)
	MOVQ	1 * SIZE(AO2), %mm5
	MOVNTQ	%mm5,   5 * SIZE(B)
	MOVQ	1 * SIZE(AO3), %mm6
	MOVNTQ	%mm6,   6 * SIZE(B)
	MOVQ	1 * SIZE(AO4), %mm7
	MOVNTQ	%mm7,   7 * SIZE(B)

	RPREFETCH	(RPREFETCHSIZE) * SIZE(AO3)

	MOVQ	2 * SIZE(AO1), %mm0
	MOVNTQ	%mm0,   8 * SIZE(B)
	MOVQ	2 * SIZE(AO2), %mm1
	MOVNTQ	%mm1,   9 * SIZE(B)

	RPREFETCH	(RPREFETCHSIZE) * SIZE(AO4)

	MOVQ	2 * SIZE(AO3), %mm2
 	MOVNTQ	%mm2,  10 * SIZE(B)
	MOVQ	2 * SIZE(AO4), %mm3
 	MOVNTQ	%mm3,  11 * SIZE(B)

	prefetchw	(WPREFETCHSIZE +  8) * SIZE(B)
	MOVQ	3 * SIZE(AO1), %mm4
	MOVNTQ	%mm4,  12 * SIZE(B)
	MOVQ	3 * SIZE(AO2), %mm5
	MOVNTQ	%mm5,  13 * SIZE(B)
	MOVQ	3 * SIZE(AO3), %mm6
	MOVNTQ	%mm6,  14 * SIZE(B)
	MOVQ	3 * SIZE(AO4), %mm7
	MOVNTQ	%mm7,  15 * SIZE(B)

	addq	$4 * SIZE, AO1
	addq	$4 * SIZE, AO2
	addq	$4 * SIZE, AO3
	addq	$4 * SIZE, AO4

	subq	$-16 * SIZE, B
	decq	I
	jg	.L14
	ALIGN_4

.L15:
	movq	M,  I
	andq	$3, I
	jle	.L17
	ALIGN_4

.L16:
	MOVQ	0 * SIZE(AO1), %mm0
	MOVQ	0 * SIZE(AO2), %mm1
	MOVQ	0 * SIZE(AO3), %mm2
	MOVQ	0 * SIZE(AO4), %mm3

	MOVNTQ	%mm0,   0 * SIZE(B)
	MOVNTQ	%mm1,   1 * SIZE(B)
	MOVNTQ	%mm2,   2 * SIZE(B)
	MOVNTQ	%mm3,   3 * SIZE(B)

	addq	$SIZE, AO1
	addq	$SIZE, AO2
	addq	$SIZE, AO3
	addq	$SIZE, AO4
	addq	$4 * SIZE, B
	decq	I
	jg	.L16
	ALIGN_4

.L17:
	decq	J
	jg	.L11
	ALIGN_4

.L20:
	testq	$2, N
	jle	.L30

	movq	A, AO1
	leaq	(A,   LDA), AO2
	leaq	(A, LDA, 2), A

	movq	M,  I
	sarq	$2, I
	jle	.L24
	ALIGN_4

.L23:
	prefetch	(RPREFETCHSIZE) * SIZE(AO1)
	MOVQ	0 * SIZE(AO1), %mm0
	prefetch	(RPREFETCHSIZE) * SIZE(AO2)
	MOVQ	0 * SIZE(AO2), %mm1
	MOVQ	1 * SIZE(AO1), %mm2
	MOVQ	1 * SIZE(AO2), %mm3
	MOVQ	2 * SIZE(AO1), %mm4
	MOVQ	2 * SIZE(AO2), %mm5
	MOVQ	3 * SIZE(AO1), %mm6
	MOVQ	3 * SIZE(AO2), %mm7

	prefetchw	(WPREFETCHSIZE +  0) * SIZE(B)

	MOVNTQ	%mm0,   0 * SIZE(B)
	MOVNTQ	%mm1,   1 * SIZE(B)
	MOVNTQ	%mm2,   2 * SIZE(B)
	MOVNTQ	%mm3,   3 * SIZE(B)
	MOVNTQ	%mm4,   4 * SIZE(B)
	MOVNTQ	%mm5,   5 * SIZE(B)
	MOVNTQ	%mm6,   6 * SIZE(B)
	MOVNTQ	%mm7,   7 * SIZE(B)

	addq	$4 * SIZE, AO1
	addq	$4 * SIZE, AO2
	subq	$-8 * SIZE, B
	decq	I
	jg	.L23
	ALIGN_4

.L24:
	movq	M,  I
	andq	$3, I
	jle	.L30
	ALIGN_4

.L25:
	MOVQ	0 * SIZE(AO1), %mm0
	MOVQ	0 * SIZE(AO2), %mm1

	MOVNTQ	%mm0,   0 * SIZE(B)
	MOVNTQ	%mm1,   1 * SIZE(B)

	addq	$SIZE, AO1
	addq	$SIZE, AO2
	addq	$2 * SIZE, B
	decq	I
	jg	.L25
	ALIGN_4

.L30:
	testq	$1, N
	jle	.L999

	movq	A, AO1

	movq	M,  I
	sarq	$2, I
	jle	.L34
	ALIGN_4

.L33:
	MOVQ	0 * SIZE(AO1), %mm0
	MOVQ	1 * SIZE(AO1), %mm1
	MOVQ	2 * SIZE(AO1), %mm2
	MOVQ	3 * SIZE(AO1), %mm3

	MOVNTQ	%mm0,   0 * SIZE(B)
	MOVNTQ	%mm1,   1 * SIZE(B)
	MOVNTQ	%mm2,   2 * SIZE(B)
	MOVNTQ	%mm3,   3 * SIZE(B)

	addq	$4 * SIZE, AO1
	subq	$-4 * SIZE, B
	decq	I
	jg	.L33
	ALIGN_4

.L34:
	movq	M,  I
	andq	$3, I
	jle	.L999
	ALIGN_4

.L35:
	MOVQ	0 * SIZE(AO1), %mm0
	addq	$SIZE, AO1

	MOVNTQ	%mm0,   0 * SIZE(B)
	addq	$1 * SIZE, B
	decq	I
	jg	.L35
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

	popq	%r12
	popq	%r13
#ifdef WINDOWS_ABI
	popq	%r14
	popq	%r15
#endif
	ret

	EPILOGUE
