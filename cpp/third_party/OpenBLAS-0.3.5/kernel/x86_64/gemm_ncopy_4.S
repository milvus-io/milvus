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
#define RPREFETCHSIZE	16
#define WPREFETCHSIZE (RPREFETCHSIZE * 4)
#define PREFETCH      prefetcht0
#define PREFETCHW     prefetcht0
#endif

#if defined(CORE2) || defined(PENRYN) || defined(DUNNINGTON) || defined(NEHALEM) || defined(SANDYBRIDGE)
#define RPREFETCHSIZE	12
#define WPREFETCHSIZE (RPREFETCHSIZE * 4)
#define PREFETCH      prefetcht0
#define PREFETCHW     prefetcht2
#endif

#ifdef ATOM
#define RPREFETCHSIZE	16
#define WPREFETCHSIZE (RPREFETCHSIZE * 4)
#define PREFETCH      prefetcht0
#define PREFETCHW     prefetcht0
#endif

#ifdef NANO
#define RPREFETCHSIZE	16
#define WPREFETCHSIZE (RPREFETCHSIZE * 4)
#define PREFETCH      prefetcht0
#define PREFETCHW     prefetcht0
#endif

#ifdef BARCELONA
#define RPREFETCHSIZE	16
#define WPREFETCHSIZE (RPREFETCHSIZE * 4)
#define PREFETCH      prefetch
#define PREFETCHW     prefetchw
#endif

#ifdef GENERIC
#define RPREFETCHSIZE	16
#define WPREFETCHSIZE (RPREFETCHSIZE * 4)
#define PREFETCH      prefetcht0
#define PREFETCHW     prefetcht0
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

	leaq	(,LDA, SIZE), LDA		# Scaling

	movq	N,  J
	sarq	$2, J
	jle	.L20
	ALIGN_4

.L12:
	movq	A, AO1
	leaq	(A,   LDA), AO2
	leaq	(A,   LDA, 2), AO3
	leaq	(AO2, LDA, 2), AO4
	leaq	(A, LDA, 4), A

	movq	M,  I
	sarq	$2, I
	jle	.L14
	ALIGN_4

.L13:
#ifndef DOUBLE
	movss	0 * SIZE(AO1), %xmm0
	movss	0 * SIZE(AO2), %xmm1
	movss	0 * SIZE(AO3), %xmm2
	movss	0 * SIZE(AO4), %xmm3

	movss	1 * SIZE(AO1), %xmm4
	movss	1 * SIZE(AO2), %xmm5
	movss	1 * SIZE(AO3), %xmm6
	movss	1 * SIZE(AO4), %xmm7

	movss	2 * SIZE(AO1), %xmm8
	movss	2 * SIZE(AO2), %xmm9
	movss	2 * SIZE(AO3), %xmm10
	movss	2 * SIZE(AO4), %xmm11

	movss	3 * SIZE(AO1), %xmm12
	movss	3 * SIZE(AO2), %xmm13
	movss	3 * SIZE(AO3), %xmm14
	movss	3 * SIZE(AO4), %xmm15

	movss	%xmm0,   0 * SIZE(B)
	movss	%xmm1,   1 * SIZE(B)
	movss	%xmm2,   2 * SIZE(B)
	movss	%xmm3,   3 * SIZE(B)
	movss	%xmm4,   4 * SIZE(B)
 	movss	%xmm5,   5 * SIZE(B)
	movss	%xmm6,   6 * SIZE(B)
	movss	%xmm7,   7 * SIZE(B)

	PREFETCH	RPREFETCHSIZE * SIZE(AO1)
	PREFETCH	RPREFETCHSIZE * SIZE(AO2)
	PREFETCH	RPREFETCHSIZE * SIZE(AO3)
	PREFETCH	RPREFETCHSIZE * SIZE(AO4)

	PREFETCHW	WPREFETCHSIZE * SIZE(B)

	movss	%xmm8,   8 * SIZE(B)
	movss	%xmm9,   9 * SIZE(B)
	movss	%xmm10, 10 * SIZE(B)
	movss	%xmm11, 11 * SIZE(B)
	movss	%xmm12, 12 * SIZE(B)
 	movss	%xmm13, 13 * SIZE(B)
	movss	%xmm14, 14 * SIZE(B)
	movss	%xmm15, 15 * SIZE(B)
#else
	PREFETCH	RPREFETCHSIZE * SIZE(AO1)
	movsd	0 * SIZE(AO1), %xmm0
	movhpd	0 * SIZE(AO2), %xmm0
	movsd	1 * SIZE(AO1), %xmm2
	movhpd	1 * SIZE(AO2), %xmm2
	PREFETCH	RPREFETCHSIZE * SIZE(AO2)
	movsd	2 * SIZE(AO1), %xmm4
	movhpd	2 * SIZE(AO2), %xmm4
	movsd	3 * SIZE(AO1), %xmm6
	movhpd	3 * SIZE(AO2), %xmm6

	PREFETCH	RPREFETCHSIZE * SIZE(AO3)
	movsd	0 * SIZE(AO3), %xmm1
	movhpd	0 * SIZE(AO4), %xmm1
	movsd	1 * SIZE(AO3), %xmm3
	movhpd	1 * SIZE(AO4), %xmm3
	PREFETCH	RPREFETCHSIZE * SIZE(AO4)
	movsd	2 * SIZE(AO3), %xmm5
	movhpd	2 * SIZE(AO4), %xmm5
	movsd	3 * SIZE(AO3), %xmm7
	movhpd	3 * SIZE(AO4), %xmm7

	PREFETCHW	WPREFETCHSIZE * SIZE(B)
	movapd	%xmm0,   0 * SIZE(B)
	movapd	%xmm1,   2 * SIZE(B)
	movapd	%xmm2,   4 * SIZE(B)
	movapd	%xmm3,   6 * SIZE(B)
#if defined(CORE2) || defined(PENRYN) || defined(DUNNINGTON)
	PREFETCHW	(WPREFETCHSIZE + 8) * SIZE(B)
#endif
	movapd	%xmm4,   8 * SIZE(B)
 	movapd	%xmm5,  10 * SIZE(B)
	movapd	%xmm6,  12 * SIZE(B)
	movapd	%xmm7,  14 * SIZE(B)
#endif

	addq	$4 * SIZE, AO1
	addq	$4 * SIZE, AO2
	addq	$4 * SIZE, AO3
	addq	$4 * SIZE, AO4

	subq	$-16 * SIZE, B
	decq	I
	jg	.L13
	ALIGN_4

.L14:
	movq	M,  I
	andq	$3, I
	jle	.L16
	ALIGN_4

.L15:
#ifndef DOUBLE
	movss	0 * SIZE(AO1), %xmm0
	movss	0 * SIZE(AO2), %xmm1
	movss	0 * SIZE(AO3), %xmm2
	movss	0 * SIZE(AO4), %xmm3

	movss	%xmm0,   0 * SIZE(B)
	movss	%xmm1,   1 * SIZE(B)
	movss	%xmm2,   2 * SIZE(B)
	movss	%xmm3,   3 * SIZE(B)
#else
	movsd	0 * SIZE(AO1), %xmm0
	movhpd	0 * SIZE(AO2), %xmm0
	movsd	0 * SIZE(AO3), %xmm1
	movhpd	0 * SIZE(AO4), %xmm1

	movapd	%xmm0,   0 * SIZE(B)
	movapd	%xmm1,   2 * SIZE(B)
#endif

	addq	$SIZE, AO1
	addq	$SIZE, AO2
	addq	$SIZE, AO3
	addq	$SIZE, AO4
	addq	$4 * SIZE, B
	decq	I
	jg	.L15
	ALIGN_4

.L16:
	decq	J
	jg	.L12
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
#ifndef DOUBLE
	movss	0 * SIZE(AO1), %xmm0
	movss	0 * SIZE(AO2), %xmm1
	movss	1 * SIZE(AO1), %xmm2
	movss	1 * SIZE(AO2), %xmm3
	movss	2 * SIZE(AO1), %xmm4
	movss	2 * SIZE(AO2), %xmm5
	movss	3 * SIZE(AO1), %xmm6
	movss	3 * SIZE(AO2), %xmm7

	movss	%xmm0,   0 * SIZE(B)
	movss	%xmm1,   1 * SIZE(B)
	movss	%xmm2,   2 * SIZE(B)
	movss	%xmm3,   3 * SIZE(B)
	movss	%xmm4,   4 * SIZE(B)
 	movss	%xmm5,   5 * SIZE(B)
	movss	%xmm6,   6 * SIZE(B)
	movss	%xmm7,   7 * SIZE(B)

#else
	movsd	0 * SIZE(AO1), %xmm0
	movhpd	0 * SIZE(AO2), %xmm0
	movsd	1 * SIZE(AO1), %xmm1
	movhpd	1 * SIZE(AO2), %xmm1

	movsd	2 * SIZE(AO1), %xmm2
	movhpd	2 * SIZE(AO2), %xmm2
	movsd	3 * SIZE(AO1), %xmm3
	movhpd	3 * SIZE(AO2), %xmm3

	movapd	%xmm0,   0 * SIZE(B)
	movapd	%xmm1,   2 * SIZE(B)
	movapd	%xmm2,   4 * SIZE(B)
	movapd	%xmm3,   6 * SIZE(B)
#endif

	PREFETCH	RPREFETCHSIZE * SIZE(AO1)
	PREFETCH	RPREFETCHSIZE * SIZE(AO2)

	PREFETCHW	WPREFETCHSIZE * SIZE(B)

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
#ifndef DOUBLE
	movss	0 * SIZE(AO1), %xmm0
	movss	0 * SIZE(AO2), %xmm1

	movss	%xmm0,   0 * SIZE(B)
	movss	%xmm1,   1 * SIZE(B)
#else
	movsd	0 * SIZE(AO1), %xmm0
	movhpd	0 * SIZE(AO2), %xmm0

	movapd	%xmm0,   0 * SIZE(B)
#endif

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
#ifndef DOUBLE
	movss	0 * SIZE(AO1), %xmm0
	movss	1 * SIZE(AO1), %xmm1
	movss	2 * SIZE(AO1), %xmm2
	movss	3 * SIZE(AO1), %xmm3

	movss	%xmm0,   0 * SIZE(B)
	movss	%xmm1,   1 * SIZE(B)
	movss	%xmm2,   2 * SIZE(B)
	movss	%xmm3,   3 * SIZE(B)
#else
	movsd	0 * SIZE(AO1), %xmm0
	movhpd	1 * SIZE(AO1), %xmm0

	movsd	2 * SIZE(AO1), %xmm1
	movhpd	3 * SIZE(AO1), %xmm1

	movapd	%xmm0,   0 * SIZE(B)
	movapd	%xmm1,   2 * SIZE(B)
#endif

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
#ifndef DOUBLE
	movss	0 * SIZE(AO1), %xmm0
	movss	%xmm0,   0 * SIZE(B)
#else
	movsd	0 * SIZE(AO1), %xmm0
	movsd	%xmm0,   0 * SIZE(B)
#endif

	addq	$SIZE, AO1
	addq	$1 * SIZE, B
	decq	I
	jg	.L35
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

	popq	%r12
	popq	%r13

#ifdef WINDOWS_ABI
	popq	%r14
	popq	%r15
#endif
	ret

	EPILOGUE
