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

#if defined(PENTIUM4) || defined(GENERIC)
#define PREFETCHSIZE	16
#define PREFETCH      prefetcht0
#define PREFETCHW     prefetcht0
#endif

#ifdef NEHALEM
#define PREFETCHSIZE	12
#define PREFETCH	prefetcht0
#define PREFETCHW	prefetcht0
#define MOVUPS_A	movups
#endif

#ifdef SANDYBRIDGE
#define PREFETCHSIZE	12
#define PREFETCH	prefetcht0
#define PREFETCHW	prefetcht0
#define MOVUPS_A	movups
#endif

#if defined(CORE2) || defined(PENRYN) || defined(DUNNINGTON)
#define PREFETCHSIZE	16
#define PREFETCH      prefetcht0
#define PREFETCHW     prefetcht0
#endif

#ifdef OPTERON
#define PREFETCHSIZE	16
#define PREFETCH      prefetch
#define PREFETCHW     prefetchw
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
#define OLD_B		40 + 40(%rsp)

#define B	%r12

#define AO1	%rsi
#define AO2	%rdi
#define LDA3	%r10
#define M8	%r11
#endif

#define I	%rax
#define B0	%rbp
#define	B3	%r13

	PROLOGUE
	PROFCODE

#ifdef WINDOWS_ABI
	pushq	%rdi
	pushq	%rsi
#endif

	pushq	%r12
	pushq	%r13
	pushq	%rbp

#ifdef WINDOWS_ABI
	movq	OLD_B,     B
#endif

	subq	$-16 * SIZE, B

	movq	M,    B3
	andq	$-2,  B3
	imulq	N,    B3

	leaq	(B, B3, SIZE), B3

	leaq	(,LDA, SIZE), LDA
	leaq	(LDA, LDA, 2), LDA3

	leaq	(, N, SIZE), M8

	cmpq	$2, N
	jl	.L40
	ALIGN_4

.L31:
	subq	$2, N

	movq	A, AO1
	leaq	(A, LDA), AO2
	leaq	(A, LDA, 2), A

	movq	B, B0
	addq	$4 * SIZE, B

	movq	M,  I
	sarq	$3, I
	jle	.L34
	ALIGN_4

.L33:
#ifdef PREFETCH
	PREFETCH	PREFETCHSIZE * 2 * SIZE(AO1)
#endif

	MOVUPS_A1(0 * SIZE, AO1, %xmm0)
	MOVUPS_A1(2 * SIZE, AO1, %xmm1)
	MOVUPS_A1(0 * SIZE, AO2, %xmm2)
	MOVUPS_A1(2 * SIZE, AO2, %xmm3)

#ifdef PREFETCHW
	PREFETCHW	(PREFETCHSIZE * 4 +  0) * SIZE(B)
#endif

	movaps	%xmm0,  -16 * SIZE(B0)
	movaps	%xmm2,  -14 * SIZE(B0)
	movaps	%xmm1,  -16 * SIZE(B0, M8, 2)
	movaps	%xmm3,  -14 * SIZE(B0, M8, 2)

	leaq	(B0, M8, 4), B0

#ifdef PREFETCH
	PREFETCH	PREFETCHSIZE * 2 * SIZE(AO2)
#endif

	MOVUPS_A1(4 * SIZE, AO1, %xmm0)
	MOVUPS_A1(6 * SIZE, AO1, %xmm1)
	MOVUPS_A1(4 * SIZE, AO2, %xmm2)
	MOVUPS_A1(6 * SIZE, AO2, %xmm3)

	movaps	%xmm0,  -16 * SIZE(B0)
	movaps	%xmm2,  -14 * SIZE(B0)
	movaps	%xmm1,  -16 * SIZE(B0, M8, 2)
	movaps	%xmm3,  -14 * SIZE(B0, M8, 2)

	leaq	(B0, M8, 4), B0

	addq	$8 * SIZE, AO1
	addq	$8 * SIZE, AO2

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

	movaps	%xmm0,  -16 * SIZE(B0)
	movaps	%xmm2,  -14 * SIZE(B0)
	movaps	%xmm1,  -16 * SIZE(B0, M8, 2)
	movaps	%xmm3,  -14 * SIZE(B0, M8, 2)

	addq	$4 * SIZE, AO1
	addq	$4 * SIZE, AO2
	leaq	(B0, M8, 4), B0
	ALIGN_4

.L36:
	testq	$2, M
	jle	.L38

	MOVUPS_A1(0 * SIZE, AO1, %xmm0)
	MOVUPS_A1(0 * SIZE, AO2, %xmm1)

	movaps	%xmm0,  -16 * SIZE(B0)
	movaps	%xmm1,  -14 * SIZE(B0)

	addq	$2 * SIZE, AO1
	addq	$2 * SIZE, AO2
	leaq	(B0, M8, 2), B0
	ALIGN_4

.L38:
	testq	$1, M
	jle	.L39

	movsd	0 * SIZE(AO1),      %xmm0
	movhpd	0 * SIZE(AO2),      %xmm0

	movaps	%xmm0,  -16 * SIZE(B3)
	subq	$-2 * SIZE, B3
	ALIGN_4

.L39:
	cmpq	$2, N
	jge	.L31
	ALIGN_4

.L40:
	cmpq	$1, N
	jl	.L999

	movq	A, AO1
	movq	B, B0

	movq	M,  I
	sarq	$3, I
	jle	.L44
	ALIGN_4

.L43:
#ifdef PREFETCH
	PREFETCH	PREFETCHSIZE * 4 * SIZE(AO1)
#endif

	MOVUPS_A1(0 * SIZE, AO1, %xmm0)
	MOVUPS_A1(2 * SIZE, AO1, %xmm1)
	MOVUPS_A1(4 * SIZE, AO1, %xmm2)
	MOVUPS_A1(6 * SIZE, AO1, %xmm3)

#ifdef PREFETCHW
	PREFETCHW	(PREFETCHSIZE * 4 +  0) * SIZE(B)
#endif

	addq	$8 * SIZE, AO1

	movaps	%xmm0,  -16 * SIZE(B0)
	movaps	%xmm1,  -16 * SIZE(B0, M8, 2)
	leaq	(B0, M8, 4), B0
	movaps	%xmm2,  -16 * SIZE(B0)
	movaps	%xmm3,  -16 * SIZE(B0, M8, 2)
	leaq	(B0, M8, 4), B0

	decq	I
	jg	.L43
	ALIGN_4

.L44:
	testq	$4, M
	jle	.L45

	MOVUPS_A1(0 * SIZE, AO1, %xmm0)
	MOVUPS_A1(2 * SIZE, AO1, %xmm1)

	addq	$4 * SIZE, AO1

	movaps	%xmm0,  -16 * SIZE(B0)
	movaps	%xmm1,  -16 * SIZE(B0, M8, 2)
	leaq	(B0, M8, 4), B0
	ALIGN_4

.L45:
	testq	$2, M
	jle	.L46

	MOVUPS_A1(0 * SIZE, AO1, %xmm0)

	movaps	%xmm0,  -16 * SIZE(B0)

	addq	$2 * SIZE, AO1
	ALIGN_4

.L46:
	testq	$1, M
	jle	.L999

	movsd	0 * SIZE(AO1),      %xmm0

	movlpd	%xmm0,  -16 * SIZE(B3)
	ALIGN_4

.L999:
	popq	%rbp
	popq	%r13
	popq	%r12

#ifdef WINDOWS_ABI
	popq	%rsi
	popq	%rdi
#endif
	ret

	EPILOGUE
