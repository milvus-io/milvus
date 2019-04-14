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

#ifdef NEHALEM
#define PREFETCHSIZE	16
#define PREFETCH	prefetcht0
#define PREFETCHW	prefetcht0
#define MOVUPS_A	movups
#endif

#ifdef SANDYBRIDGE
#define PREFETCHSIZE	16
#define PREFETCH	prefetcht0
#define PREFETCHW	prefetcht0
#define MOVUPS_A	movups
#endif

#ifdef MOVUPS_A
#define MOVUPS_A1(OFF, ADDR, REGS)		MOVUPS_A	OFF(ADDR), REGS
#define MOVUPS_A2(OFF, ADDR, BASE, SCALE, REGS)	MOVUPS_A	OFF(ADDR, BASE, SCALE), REGS
#else
#define MOVUPS_A1(OFF, ADDR, REGS)		movsd	OFF(ADDR), REGS; movhps	OFF + 8(ADDR), REGS
#define MOVUPS_A2(OFF, ADDR, BASE, SCALE, REGS)	movsd	OFF(ADDR, BASE, SCALE), REGS; movhps	OFF + 8(ADDR, BASE, SCALE), REGS
#endif

#ifndef WINDOWS_ABI

#define N	ARG1	/* rsi */
#define M	ARG2	/* rdi */
#define A	ARG3	/* rdx */
#define LDA	ARG4	/* rcx */
#define B	ARG5	/* r8  */

#define AO1	%r9
#define AO2	%r10
#define LDA3	%r11
#define M8	%r12

#else

#define N	ARG1	/* rdx */
#define M	ARG2	/* rcx */
#define A	ARG3	/* r8  */
#define LDA	ARG4	/* r9  */
#define OLD_B		40 + 56(%rsp)

#define B	%r12

#define AO1	%rsi
#define AO2	%rdi
#define LDA3	%r10
#define M8	%r11
#endif

#define I	%rax

#define B0	%rbp
#define	B1	%r13
#define	B2	%r14
#define	B3	%r15

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

#ifdef WINDOWS_ABI
	movq	OLD_B,     B
#endif

	subq	$-16 * SIZE, B

	movq	M,    B1
	movq	M,    B2
	movq	M,    B3

	andq	$-8,  B1
	andq	$-4,  B2
	andq	$-2,  B3

	imulq	N,    B1
	imulq	N,    B2
	imulq	N,    B3

	leaq	(B, B1, SIZE), B1
	leaq	(B, B2, SIZE), B2
	leaq	(B, B3, SIZE), B3

	leaq	(,LDA, SIZE), LDA
	leaq	(LDA, LDA, 2), LDA3

	leaq	(, N, SIZE), M8

	cmpq	$8, N
	jl	.L20
	ALIGN_4

.L11:
	subq	$8, N

	movq	A, AO1
	leaq	(A, LDA, 4), AO2
	leaq	(A, LDA, 8), A

	movq	B, B0
	addq	$64 * SIZE, B

	movq	M, I
	sarq	$3, I
	jle	.L14
	ALIGN_4

.L13:
#ifdef PREFETCH
	PREFETCH	PREFETCHSIZE * SIZE(AO1)
#endif

	MOVUPS_A1(0 * SIZE, AO1, %xmm0)
	MOVUPS_A1(2 * SIZE, AO1, %xmm1)
	MOVUPS_A1(4 * SIZE, AO1, %xmm2)
	MOVUPS_A1(6 * SIZE, AO1, %xmm3)

#ifdef PREFETCHW
	PREFETCHW	48 * SIZE(B0)
#endif

	movaps	%xmm0,  -16 * SIZE(B0)
	movaps	%xmm1,  -14 * SIZE(B0)
	movaps	%xmm2,  -12 * SIZE(B0)
	movaps	%xmm3,  -10 * SIZE(B0)

#ifdef PREFETCH
	PREFETCH	PREFETCHSIZE * SIZE(AO1, LDA)
#endif

	MOVUPS_A2(0 * SIZE, AO1, LDA, 1, %xmm0)
	MOVUPS_A2(2 * SIZE, AO1, LDA, 1, %xmm1)
	MOVUPS_A2(4 * SIZE, AO1, LDA, 1, %xmm2)
	MOVUPS_A2(6 * SIZE, AO1, LDA, 1, %xmm3)

#ifdef PREFETCHW
	PREFETCHW	56 * SIZE(B0)
#endif

	movaps	%xmm0,   -8 * SIZE(B0)
	movaps	%xmm1,   -6 * SIZE(B0)
	movaps	%xmm2,   -4 * SIZE(B0)
	movaps	%xmm3,   -2 * SIZE(B0)

#ifdef PREFETCH
	PREFETCH	PREFETCHSIZE * SIZE(AO1, LDA, 2)
#endif

	MOVUPS_A2(0 * SIZE, AO1, LDA, 2, %xmm0)
	MOVUPS_A2(2 * SIZE, AO1, LDA, 2, %xmm1)
	MOVUPS_A2(4 * SIZE, AO1, LDA, 2, %xmm2)
	MOVUPS_A2(6 * SIZE, AO1, LDA, 2, %xmm3)

#ifdef PREFETCHW
	PREFETCHW	64 * SIZE(B0)
#endif

	movaps	%xmm0,    0 * SIZE(B0)
	movaps	%xmm1,    2 * SIZE(B0)
	movaps	%xmm2,    4 * SIZE(B0)
	movaps	%xmm3,    6 * SIZE(B0)

#ifdef PREFETCH
	PREFETCH	PREFETCHSIZE * SIZE(AO1, LDA3)
#endif

	MOVUPS_A2(0 * SIZE, AO1, LDA3, 1, %xmm0)
	MOVUPS_A2(2 * SIZE, AO1, LDA3, 1, %xmm1)
	MOVUPS_A2(4 * SIZE, AO1, LDA3, 1, %xmm2)
	MOVUPS_A2(6 * SIZE, AO1, LDA3, 1, %xmm3)

#ifdef PREFETCHW
	PREFETCHW	72 * SIZE(B0)
#endif

	movaps	%xmm0,    8 * SIZE(B0)
	movaps	%xmm1,   10 * SIZE(B0)
	movaps	%xmm2,   12 * SIZE(B0)
	movaps	%xmm3,   14 * SIZE(B0)

#ifdef PREFETCH
	PREFETCH	PREFETCHSIZE * SIZE(AO2)
#endif

	MOVUPS_A1(0 * SIZE, AO2, %xmm0)
	MOVUPS_A1(2 * SIZE, AO2, %xmm1)
	MOVUPS_A1(4 * SIZE, AO2, %xmm2)
	MOVUPS_A1(6 * SIZE, AO2, %xmm3)

#ifdef PREFETCHW
	PREFETCHW	80 * SIZE(B0)
#endif

	movaps	%xmm0,   16 * SIZE(B0)
	movaps	%xmm1,   18 * SIZE(B0)
	movaps	%xmm2,   20 * SIZE(B0)
	movaps	%xmm3,   22 * SIZE(B0)

#ifdef PREFETCH
	PREFETCH	PREFETCHSIZE * SIZE(AO2, LDA)
#endif

	MOVUPS_A2(0 * SIZE, AO2, LDA, 1, %xmm0)
	MOVUPS_A2(2 * SIZE, AO2, LDA, 1, %xmm1)
	MOVUPS_A2(4 * SIZE, AO2, LDA, 1, %xmm2)
	MOVUPS_A2(6 * SIZE, AO2, LDA, 1, %xmm3)

#ifdef PREFETCHW
	PREFETCHW	88 * SIZE(B0)
#endif

	movaps	%xmm0,   24 * SIZE(B0)
	movaps	%xmm1,   26 * SIZE(B0)
	movaps	%xmm2,   28 * SIZE(B0)
	movaps	%xmm3,   30 * SIZE(B0)

#ifdef PREFETCH
	PREFETCH	PREFETCHSIZE * SIZE(AO2, LDA, 2)
#endif

	MOVUPS_A2(0 * SIZE, AO2, LDA, 2, %xmm0)
	MOVUPS_A2(2 * SIZE, AO2, LDA, 2, %xmm1)
	MOVUPS_A2(4 * SIZE, AO2, LDA, 2, %xmm2)
	MOVUPS_A2(6 * SIZE, AO2, LDA, 2, %xmm3)

#ifdef PREFETCHW
	PREFETCHW	96 * SIZE(B0)
#endif

	movaps	%xmm0,   32 * SIZE(B0)
	movaps	%xmm1,   34 * SIZE(B0)
	movaps	%xmm2,   36 * SIZE(B0)
	movaps	%xmm3,   38 * SIZE(B0)

#ifdef PREFETCH
	PREFETCH	PREFETCHSIZE * SIZE(AO2, LDA3)
#endif

	MOVUPS_A2(0 * SIZE, AO2, LDA3, 1, %xmm0)
	MOVUPS_A2(2 * SIZE, AO2, LDA3, 1, %xmm1)
	MOVUPS_A2(4 * SIZE, AO2, LDA3, 1, %xmm2)
	MOVUPS_A2(6 * SIZE, AO2, LDA3, 1, %xmm3)

#ifdef PREFETCHW
	PREFETCHW	104 * SIZE(B0)
#endif

	movaps	%xmm0,   40 * SIZE(B0)
	movaps	%xmm1,   42 * SIZE(B0)
	movaps	%xmm2,   44 * SIZE(B0)
	movaps	%xmm3,   46 * SIZE(B0)

	addq	$8 * SIZE, AO1
	addq	$8 * SIZE, AO2
	leaq	(B0, M8, 8), B0

	decq	I
	jg	.L13
	ALIGN_4

.L14:
	testq	$4, M
	jle	.L16

	MOVUPS_A1(0 * SIZE, AO1, %xmm0)
	MOVUPS_A1(2 * SIZE, AO1, %xmm1)
	MOVUPS_A2(0 * SIZE, AO1, LDA, 1, %xmm2)
	MOVUPS_A2(2 * SIZE, AO1, LDA, 1, %xmm3)

	movaps	%xmm0,  -16 * SIZE(B1)
	movaps	%xmm1,  -14 * SIZE(B1)
	movaps	%xmm2,  -12 * SIZE(B1)
	movaps	%xmm3,  -10 * SIZE(B1)

	MOVUPS_A2(0 * SIZE, AO1, LDA,  2, %xmm0)
	MOVUPS_A2(2 * SIZE, AO1, LDA,  2, %xmm1)
	MOVUPS_A2(0 * SIZE, AO1, LDA3, 1, %xmm2)
	MOVUPS_A2(2 * SIZE, AO1, LDA3, 1, %xmm3)

	movaps	%xmm0,   -8 * SIZE(B1)
	movaps	%xmm1,   -6 * SIZE(B1)
	movaps	%xmm2,   -4 * SIZE(B1)
	movaps	%xmm3,   -2 * SIZE(B1)

	MOVUPS_A1(0 * SIZE, AO2, %xmm0)
	MOVUPS_A1(2 * SIZE, AO2, %xmm1)
	MOVUPS_A2(0 * SIZE, AO2, LDA, 1, %xmm2)
	MOVUPS_A2(2 * SIZE, AO2, LDA, 1, %xmm3)

	movaps	%xmm0,    0 * SIZE(B1)
	movaps	%xmm1,    2 * SIZE(B1)
	movaps	%xmm2,    4 * SIZE(B1)
	movaps	%xmm3,    6 * SIZE(B1)

	MOVUPS_A2(0 * SIZE, AO2, LDA,  2, %xmm0)
	MOVUPS_A2(2 * SIZE, AO2, LDA,  2, %xmm1)
	MOVUPS_A2(0 * SIZE, AO2, LDA3, 1, %xmm2)
	MOVUPS_A2(2 * SIZE, AO2, LDA3, 1, %xmm3)

	movaps	%xmm0,    8 * SIZE(B1)
	movaps	%xmm1,   10 * SIZE(B1)
	movaps	%xmm2,   12 * SIZE(B1)
	movaps	%xmm3,   14 * SIZE(B1)

	addq	$4 * SIZE, AO1
	addq	$4 * SIZE, AO2
	subq	$-32 * SIZE, B1
	ALIGN_4

.L16:
	testq	$2, M
	jle	.L18

	MOVUPS_A1(0 * SIZE, AO1, %xmm0)
	MOVUPS_A2(0 * SIZE, AO1, LDA,  1, %xmm1)
	MOVUPS_A2(0 * SIZE, AO1, LDA,  2, %xmm2)
	MOVUPS_A2(0 * SIZE, AO1, LDA3, 1, %xmm3)

	movaps	%xmm0,  -16 * SIZE(B2)
	movaps	%xmm1,  -14 * SIZE(B2)
	movaps	%xmm2,  -12 * SIZE(B2)
	movaps	%xmm3,  -10 * SIZE(B2)

	MOVUPS_A1(0 * SIZE, AO2, %xmm0)
	MOVUPS_A2(0 * SIZE, AO2, LDA,  1, %xmm1)
	MOVUPS_A2(0 * SIZE, AO2, LDA,  2, %xmm2)
	MOVUPS_A2(0 * SIZE, AO2, LDA3, 1, %xmm3)

	movaps	%xmm0,   -8 * SIZE(B2)
	movaps	%xmm1,   -6 * SIZE(B2)
	movaps	%xmm2,   -4 * SIZE(B2)
	movaps	%xmm3,   -2 * SIZE(B2)

	addq	$2 * SIZE, AO1
	addq	$2 * SIZE, AO2
	subq	$-16 * SIZE, B2
	ALIGN_4

.L18:
	testq	$1, M
	jle	.L19

	movsd	0 * SIZE(AO1),         %xmm0
	movsd	0 * SIZE(AO1, LDA),    %xmm1
	movsd	0 * SIZE(AO1, LDA, 2), %xmm2
	movsd	0 * SIZE(AO1, LDA3),   %xmm3

	unpcklpd %xmm1, %xmm0
	unpcklpd %xmm3, %xmm2

	movaps	%xmm0,  -16 * SIZE(B3)
	movaps	%xmm2,  -14 * SIZE(B3)

	movsd	0 * SIZE(AO2),         %xmm0
	movsd	0 * SIZE(AO2, LDA),    %xmm1
	movsd	0 * SIZE(AO2, LDA, 2), %xmm2
	movsd	0 * SIZE(AO2, LDA3),   %xmm3

	unpcklpd %xmm1, %xmm0
	unpcklpd %xmm3, %xmm2

	movaps	%xmm0,  -12 * SIZE(B3)
	movaps	%xmm2,  -10 * SIZE(B3)

	subq	$-8 * SIZE, B3
	ALIGN_4

.L19:
	cmpq	$8, N
	jge	.L11
	ALIGN_4

.L20:
	cmpq	$4, N
	jl	.L30

	subq	$4, N

	movq	A, AO1
	leaq	(A, LDA, 2), AO2
	leaq	(A, LDA, 4), A

	movq	B, B0
	addq	$32 * SIZE, B

	movq	M, I
	sarq	$3, I
	jle	.L24
	ALIGN_4

.L23:
#ifdef PREFETCH
	PREFETCH	PREFETCHSIZE * SIZE(AO1)
#endif

	MOVUPS_A1(0 * SIZE, AO1, %xmm0)
	MOVUPS_A1(2 * SIZE, AO1, %xmm1)
	MOVUPS_A1(4 * SIZE, AO1, %xmm2)
	MOVUPS_A1(6 * SIZE, AO1, %xmm3)

#ifdef PREFETCHW
	PREFETCHW	16 * SIZE(B0)
#endif

	movaps	%xmm0,  -16 * SIZE(B0)
	movaps	%xmm1,  -14 * SIZE(B0)
	movaps	%xmm2,  -12 * SIZE(B0)
	movaps	%xmm3,  -10 * SIZE(B0)

#ifdef PREFETCH
	PREFETCH	PREFETCHSIZE * SIZE(AO1, LDA)
#endif

	MOVUPS_A2(0 * SIZE, AO1, LDA, 1, %xmm0)
	MOVUPS_A2(2 * SIZE, AO1, LDA, 1, %xmm1)
	MOVUPS_A2(4 * SIZE, AO1, LDA, 1, %xmm2)
	MOVUPS_A2(6 * SIZE, AO1, LDA, 1, %xmm3)

#ifdef PREFETCHW
	PREFETCHW	24 * SIZE(B0)
#endif

	movaps	%xmm0,   -8 * SIZE(B0)
	movaps	%xmm1,   -6 * SIZE(B0)
	movaps	%xmm2,   -4 * SIZE(B0)
	movaps	%xmm3,   -2 * SIZE(B0)

#ifdef PREFETCH
	PREFETCH	PREFETCHSIZE * SIZE(AO1, LDA, 2)
#endif

	MOVUPS_A1(0 * SIZE, AO2, %xmm0)
	MOVUPS_A1(2 * SIZE, AO2, %xmm1)
	MOVUPS_A1(4 * SIZE, AO2, %xmm2)
	MOVUPS_A1(6 * SIZE, AO2, %xmm3)

#ifdef PREFETCHW
	PREFETCHW	32 * SIZE(B0)
#endif

	movaps	%xmm0,    0 * SIZE(B0)
	movaps	%xmm1,    2 * SIZE(B0)
	movaps	%xmm2,    4 * SIZE(B0)
	movaps	%xmm3,    6 * SIZE(B0)

#ifdef PREFETCH
	PREFETCH	PREFETCHSIZE * SIZE(AO1, LDA3)
#endif

	MOVUPS_A2(0 * SIZE, AO2, LDA, 1, %xmm0)
	MOVUPS_A2(2 * SIZE, AO2, LDA, 1, %xmm1)
	MOVUPS_A2(4 * SIZE, AO2, LDA, 1, %xmm2)
	MOVUPS_A2(6 * SIZE, AO2, LDA, 1, %xmm3)

#ifdef PREFETCHW
	PREFETCHW	40 * SIZE(B0)
#endif

	movaps	%xmm0,    8 * SIZE(B0)
	movaps	%xmm1,   10 * SIZE(B0)
	movaps	%xmm2,   12 * SIZE(B0)
	movaps	%xmm3,   14 * SIZE(B0)

	addq	$8 * SIZE, AO1
	addq	$8 * SIZE, AO2
	leaq	(B0, M8, 8), B0

	decq	I
	jg	.L23
	ALIGN_4

.L24:
	testq	$4, M
	jle	.L26

	MOVUPS_A1(0 * SIZE, AO1, %xmm0)
	MOVUPS_A1(2 * SIZE, AO1, %xmm1)
	MOVUPS_A2(0 * SIZE, AO1, LDA, 1, %xmm2)
	MOVUPS_A2(2 * SIZE, AO1, LDA, 1, %xmm3)

	movaps	%xmm0,  -16 * SIZE(B1)
	movaps	%xmm1,  -14 * SIZE(B1)
	movaps	%xmm2,  -12 * SIZE(B1)
	movaps	%xmm3,  -10 * SIZE(B1)

	MOVUPS_A1(0 * SIZE, AO2, %xmm0)
	MOVUPS_A1(2 * SIZE, AO2, %xmm1)
	MOVUPS_A2(0 * SIZE, AO2, LDA, 1, %xmm2)
	MOVUPS_A2(2 * SIZE, AO2, LDA, 1, %xmm3)

	movaps	%xmm0,   -8 * SIZE(B1)
	movaps	%xmm1,   -6 * SIZE(B1)
	movaps	%xmm2,   -4 * SIZE(B1)
	movaps	%xmm3,   -2 * SIZE(B1)

	addq	$4 * SIZE, AO1
	addq	$4 * SIZE, AO2
	subq	$-16 * SIZE, B1
	ALIGN_4

.L26:
	testq	$2, M
	jle	.L28

	MOVUPS_A1(0 * SIZE, AO1, %xmm0)
	MOVUPS_A2(0 * SIZE, AO1, LDA,  1, %xmm1)
	MOVUPS_A1(0 * SIZE, AO2, %xmm2)
	MOVUPS_A2(0 * SIZE, AO2, LDA,  1, %xmm3)

	movaps	%xmm0,  -16 * SIZE(B2)
	movaps	%xmm1,  -14 * SIZE(B2)
	movaps	%xmm2,  -12 * SIZE(B2)
	movaps	%xmm3,  -10 * SIZE(B2)

	addq	$2 * SIZE, AO1
	addq	$2 * SIZE, AO2
	subq	$-8 * SIZE, B2
	ALIGN_4

.L28:
	testq	$1, M
	jle	.L30

	movsd	0 * SIZE(AO1),      %xmm0
	movsd	0 * SIZE(AO1, LDA), %xmm1
	movsd	0 * SIZE(AO2),      %xmm2
	movsd	0 * SIZE(AO2, LDA), %xmm3

	unpcklpd %xmm1, %xmm0
	unpcklpd %xmm3, %xmm2

	movaps	%xmm0,  -16 * SIZE(B3)
	movaps	%xmm2,  -14 * SIZE(B3)
	subq	$-4 * SIZE, B3
	ALIGN_4

.L30:
	cmpq	$2, N
	jl	.L40

	subq	$2, N

	movq	A, AO1
	leaq	(A, LDA), AO2
	leaq	(A, LDA, 2), A

	movq	B, B0
	addq	$16 * SIZE, B

	movq	M, I
	sarq	$3, I
	jle	.L34
	ALIGN_4

.L33:
#ifdef PREFETCH
	PREFETCH	PREFETCHSIZE * SIZE(AO1)
#endif

	MOVUPS_A1(0 * SIZE, AO1, %xmm0)
	MOVUPS_A1(2 * SIZE, AO1, %xmm1)
	MOVUPS_A1(4 * SIZE, AO1, %xmm2)
	MOVUPS_A1(6 * SIZE, AO1, %xmm3)

#ifdef PREFETCHW
	PREFETCHW	 0 * SIZE(B0)
#endif

	movaps	%xmm0,  -16 * SIZE(B0)
	movaps	%xmm1,  -14 * SIZE(B0)
	movaps	%xmm2,  -12 * SIZE(B0)
	movaps	%xmm3,  -10 * SIZE(B0)

#ifdef PREFETCH
	PREFETCH	PREFETCHSIZE * SIZE(AO2)
#endif

	MOVUPS_A1(0 * SIZE, AO2, %xmm0)
	MOVUPS_A1(2 * SIZE, AO2, %xmm1)
	MOVUPS_A1(4 * SIZE, AO2, %xmm2)
	MOVUPS_A1(6 * SIZE, AO2, %xmm3)

#ifdef PREFETCHW
	PREFETCHW	 8 * SIZE(B0)
#endif

	movaps	%xmm0,   -8 * SIZE(B0)
	movaps	%xmm1,   -6 * SIZE(B0)
	movaps	%xmm2,   -4 * SIZE(B0)
	movaps	%xmm3,   -2 * SIZE(B0)

	addq	$8 * SIZE, AO1
	addq	$8 * SIZE, AO2
	leaq	(B0, M8, 8), B0

	decq	I
	jg	.L33
	ALIGN_4

.L34:
	testq	$4, M
	jle	.L36

	MOVUPS_A1(0 * SIZE, AO1, %xmm0)
	MOVUPS_A1(2 * SIZE, AO1, %xmm1)
	MOVUPS_A1(0 * SIZE, AO2, %xmm2)
	MOVUPS_A1(2 * SIZE, AO2, %xmm3)

	movaps	%xmm0,  -16 * SIZE(B1)
	movaps	%xmm1,  -14 * SIZE(B1)
	movaps	%xmm2,  -12 * SIZE(B1)
	movaps	%xmm3,  -10 * SIZE(B1)

	addq	$4 * SIZE, AO1
	addq	$4 * SIZE, AO2
	subq	$-8 * SIZE, B1
	ALIGN_4

.L36:
	testq	$2, M
	jle	.L38

	MOVUPS_A1(0 * SIZE, AO1, %xmm0)
	MOVUPS_A1(0 * SIZE, AO2, %xmm1)

	movaps	%xmm0,  -16 * SIZE(B2)
	movaps	%xmm1,  -14 * SIZE(B2)

	addq	$2 * SIZE, AO1
	addq	$2 * SIZE, AO2
	subq	$-4 * SIZE, B2
	ALIGN_4

.L38:
	testq	$1, M
	jle	.L40

	movsd	0 * SIZE(AO1),      %xmm0
	movsd	0 * SIZE(AO2),      %xmm1

	unpcklpd %xmm1, %xmm0

	movaps	%xmm0,  -16 * SIZE(B3)
	subq	$-2 * SIZE, B3
	ALIGN_4

.L40:
	cmpq	$1, N
	jl	.L999

	movq	A, AO1

	movq	B, B0

	movq	M, I
	sarq	$3, I
	jle	.L44
	ALIGN_4

.L43:
#ifdef PREFETCH
	PREFETCH	PREFETCHSIZE * 8 * SIZE(AO1)
#endif

	MOVUPS_A1(0 * SIZE, AO1, %xmm0)
	MOVUPS_A1(2 * SIZE, AO1, %xmm1)
	MOVUPS_A1(4 * SIZE, AO1, %xmm2)
	MOVUPS_A1(6 * SIZE, AO1, %xmm3)

#ifdef PREFETCHW
	PREFETCHW	-8 * SIZE(B0)
#endif

	movaps	%xmm0,  -16 * SIZE(B0)
	movaps	%xmm1,  -14 * SIZE(B0)
	movaps	%xmm2,  -12 * SIZE(B0)
	movaps	%xmm3,  -10 * SIZE(B0)

	addq	$8 * SIZE, AO1
	leaq	(B0, M8, 8), B0

	decq	I
	jg	.L43
	ALIGN_4

.L44:
	testq	$4, M
	jle	.L45

	MOVUPS_A1(0 * SIZE, AO1, %xmm0)
	MOVUPS_A1(2 * SIZE, AO1, %xmm1)

	movaps	%xmm0, -16 * SIZE(B1)
	movaps	%xmm1, -14 * SIZE(B1)

	addq	$4 * SIZE, AO1
	subq	$-4 * SIZE, B1
	ALIGN_4

.L45:
	testq	$2, M
	jle	.L46

	MOVUPS_A1(0 * SIZE, AO1, %xmm0)

	movaps	%xmm0,  -16 * SIZE(B2)

	addq	$2 * SIZE, AO1
	subq	$-2 * SIZE, B2
	ALIGN_4

.L46:
	testq	$1, M
	jle	.L999

	movsd	0 * SIZE(AO1),      %xmm0

	movlpd	%xmm0,  -16 * SIZE(B3)
	jmp	.L999
	ALIGN_4

.L999:
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
