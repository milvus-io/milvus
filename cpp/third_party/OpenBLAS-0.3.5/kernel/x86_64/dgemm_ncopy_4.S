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

#if defined(CORE2) || defined(PENRYN) || defined(DUNNINGTON) || defined(NEHALEM) || defined(SANDYBRIDGE)
#define PREFETCHSIZE	16
#define PREFETCH      prefetcht0
#define PREFETCHW     prefetcht0
#endif

#ifdef ATOM
#define PREFETCHSIZE	16
#define PREFETCH      prefetcht0
#define PREFETCHW     prefetcht0
#endif

#ifdef NANO
#define PREFETCHSIZE	16
#define PREFETCH      prefetcht0
#define PREFETCHW     prefetcht0
#endif

#ifdef OPTERON
#define PREFETCHSIZE	16
#define PREFETCH      prefetch
#define PREFETCHW     prefetchw
#endif

#ifdef GENERIC
#define PREFETCHSIZE	16
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
#define MM	%r13

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

	movq	OLD_B,     B
#endif

	leaq	(,LDA, SIZE), LDA
	subq	$-16 * SIZE, B

	movq	M, MM
	leaq	-1(M), %rax
	testq	$SIZE, A
	cmovne	%rax, MM

	testq	$SIZE, LDA
	jne	.L50

	movq	N,  J
	sarq	$2, J
	jle	.L20
	ALIGN_4

.L11:
	movq	A, AO1
	leaq	(A, LDA, 2), AO2
	leaq	(A, LDA, 4), A

	testq	$SIZE, A
	je	.L12

	movsd	0 * SIZE(AO1),      %xmm0
	movsd	0 * SIZE(AO1, LDA), %xmm1
	movsd	0 * SIZE(AO2),      %xmm2
	movsd	0 * SIZE(AO2, LDA), %xmm3

	unpcklpd %xmm1, %xmm0
	unpcklpd %xmm3, %xmm2

	movapd	%xmm0,  -16 * SIZE(B)
	movapd	%xmm2,  -14 * SIZE(B)

	addq	$1 * SIZE, AO1
	addq	$1 * SIZE, AO2
	subq	$-4 * SIZE, B
	ALIGN_3

.L12:
	movq	MM, I
	sarq	$3, I
	jle	.L14
	ALIGN_4

.L13:
#ifdef PREFETCH
	PREFETCH	PREFETCHSIZE * SIZE(AO1)
#endif

	movapd	0 * SIZE(AO1),      %xmm0
	movapd	0 * SIZE(AO1, LDA), %xmm1
	movapd	0 * SIZE(AO2),      %xmm2
	movapd	0 * SIZE(AO2, LDA), %xmm3

	movapd	 %xmm0, %xmm4
	unpcklpd %xmm1, %xmm0
	movapd	 %xmm2, %xmm6
	unpcklpd %xmm3, %xmm2

	unpckhpd %xmm1, %xmm4
	unpckhpd %xmm3, %xmm6

#ifdef PREFETCHW
	PREFETCHW	(PREFETCHSIZE * 4 +  0) * SIZE(B)
#endif

	movapd	%xmm0,  -16 * SIZE(B)
	movapd	%xmm2,  -14 * SIZE(B)
	movapd	%xmm4,  -12 * SIZE(B)
	movapd	%xmm6,  -10 * SIZE(B)

#ifdef PREFETCH
	PREFETCH	PREFETCHSIZE * SIZE(AO1, LDA)
#endif

	movapd	2 * SIZE(AO1),      %xmm0
	movapd	2 * SIZE(AO1, LDA), %xmm1
	movapd	2 * SIZE(AO2),      %xmm2
	movapd	2 * SIZE(AO2, LDA), %xmm3

	movapd	 %xmm0, %xmm4
	unpcklpd %xmm1, %xmm0
	movapd	 %xmm2, %xmm6
	unpcklpd %xmm3, %xmm2

	unpckhpd %xmm1, %xmm4
	unpckhpd %xmm3, %xmm6

#ifdef PREFETCHW
	PREFETCHW	(PREFETCHSIZE * 4 +  8) * SIZE(B)
#endif

	movapd	%xmm0,  -8 * SIZE(B)
	movapd	%xmm2,  -6 * SIZE(B)
	movapd	%xmm4,  -4 * SIZE(B)
	movapd	%xmm6,  -2 * SIZE(B)

#ifdef PREFETCH
	PREFETCH	PREFETCHSIZE * SIZE(AO2)
#endif

	movapd	4 * SIZE(AO1),      %xmm0
	movapd	4 * SIZE(AO1, LDA), %xmm1
	movapd	4 * SIZE(AO2),      %xmm2
	movapd	4 * SIZE(AO2, LDA), %xmm3

	movapd	 %xmm0, %xmm4
	unpcklpd %xmm1, %xmm0
	movapd	 %xmm2, %xmm6
	unpcklpd %xmm3, %xmm2

	unpckhpd %xmm1, %xmm4
	unpckhpd %xmm3, %xmm6

#ifdef PREFETCHW
	PREFETCHW	(PREFETCHSIZE * 4 + 16) * SIZE(B)
#endif

	movapd	%xmm0,   0 * SIZE(B)
	movapd	%xmm2,   2 * SIZE(B)
	movapd	%xmm4,   4 * SIZE(B)
	movapd	%xmm6,   6 * SIZE(B)

#ifdef PREFETCH
	PREFETCH	PREFETCHSIZE * SIZE(AO2, LDA)
#endif

	movapd	6 * SIZE(AO1),      %xmm0
	movapd	6 * SIZE(AO1, LDA), %xmm1
	movapd	6 * SIZE(AO2),      %xmm2
	movapd	6 * SIZE(AO2, LDA), %xmm3

	movapd	 %xmm0, %xmm4
	unpcklpd %xmm1, %xmm0
	movapd	 %xmm2, %xmm6
	unpcklpd %xmm3, %xmm2

	unpckhpd %xmm1, %xmm4
	unpckhpd %xmm3, %xmm6

#ifdef PREFETCHW
	PREFETCHW	(PREFETCHSIZE * 4 + 24) * SIZE(B)
#endif

	movapd	%xmm0,   8 * SIZE(B)
	movapd	%xmm2,  10 * SIZE(B)
	movapd	%xmm4,  12 * SIZE(B)
	movapd	%xmm6,  14 * SIZE(B)

	addq	$8 * SIZE, AO1
	addq	$8 * SIZE, AO2
	subq	$-32 * SIZE, B

	decq	I
	jg	.L13
	ALIGN_4

.L14:
	testq	$4, MM
	jle	.L16

	movapd	0 * SIZE(AO1),      %xmm0
	movapd	0 * SIZE(AO1, LDA), %xmm1
	movapd	0 * SIZE(AO2),      %xmm2
	movapd	0 * SIZE(AO2, LDA), %xmm3

	movapd	 %xmm0, %xmm4
	unpcklpd %xmm1, %xmm0
	movapd	 %xmm2, %xmm6
	unpcklpd %xmm3, %xmm2

	unpckhpd %xmm1, %xmm4
	unpckhpd %xmm3, %xmm6

	movapd	%xmm0,  -16 * SIZE(B)
	movapd	%xmm2,  -14 * SIZE(B)
	movapd	%xmm4,  -12 * SIZE(B)
	movapd	%xmm6,  -10 * SIZE(B)

	movapd	2 * SIZE(AO1),      %xmm0
	movapd	2 * SIZE(AO1, LDA), %xmm1
	movapd	2 * SIZE(AO2),      %xmm2
	movapd	2 * SIZE(AO2, LDA), %xmm3

	movapd	 %xmm0, %xmm4
	unpcklpd %xmm1, %xmm0
	movapd	 %xmm2, %xmm6
	unpcklpd %xmm3, %xmm2

	unpckhpd %xmm1, %xmm4
	unpckhpd %xmm3, %xmm6

	movapd	%xmm0,  -8 * SIZE(B)
	movapd	%xmm2,  -6 * SIZE(B)
	movapd	%xmm4,  -4 * SIZE(B)
	movapd	%xmm6,  -2 * SIZE(B)

	addq	$4 * SIZE, AO1
	addq	$4 * SIZE, AO2
	subq	$-16 * SIZE, B
	ALIGN_4

.L16:
	testq	$2, MM
	jle	.L18

	movapd	0 * SIZE(AO1),      %xmm0
	movapd	0 * SIZE(AO1, LDA), %xmm1
	movapd	0 * SIZE(AO2),      %xmm2
	movapd	0 * SIZE(AO2, LDA), %xmm3

	movapd	 %xmm0, %xmm4
	unpcklpd %xmm1, %xmm0
	movapd	 %xmm2, %xmm6
	unpcklpd %xmm3, %xmm2

	unpckhpd %xmm1, %xmm4
	unpckhpd %xmm3, %xmm6

	movapd	%xmm0,  -16 * SIZE(B)
	movapd	%xmm2,  -14 * SIZE(B)
	movapd	%xmm4,  -12 * SIZE(B)
	movapd	%xmm6,  -10 * SIZE(B)

	addq	$2 * SIZE, AO1
	addq	$2 * SIZE, AO2
	subq	$-8 * SIZE, B
	ALIGN_4

.L18:
	testq	$1, MM
	jle	.L19

	movsd	0 * SIZE(AO1),      %xmm0
	movsd	0 * SIZE(AO1, LDA), %xmm1
	movsd	0 * SIZE(AO2),      %xmm2
	movsd	0 * SIZE(AO2, LDA), %xmm3

	unpcklpd %xmm1, %xmm0
	unpcklpd %xmm3, %xmm2

	movapd	%xmm0,  -16 * SIZE(B)
	movapd	%xmm2,  -14 * SIZE(B)
	subq	$-4 * SIZE, B
	ALIGN_4

.L19:
	decq	J
	jg	.L11
	ALIGN_4

.L20:
	testq	$2, N
	jle	.L30

	movq	A, AO1
	leaq	(A, LDA), AO2
	leaq	(A, LDA, 2), A

	testq	$SIZE, A
	je	.L22

	movsd	0 * SIZE(AO1),      %xmm0
	movsd	0 * SIZE(AO2),      %xmm1

	unpcklpd %xmm1, %xmm0

	movapd	%xmm0,  -16 * SIZE(B)

	addq	$1 * SIZE, AO1
	addq	$1 * SIZE, AO2
	subq	$-2 * SIZE, B
	ALIGN_3

.L22:
	movq	MM, I
	sarq	$3, I
	jle	.L24
	ALIGN_4

.L23:
#ifdef PREFETCH
	PREFETCH	PREFETCHSIZE * 2 * SIZE(AO1)
#endif

	movapd	0 * SIZE(AO1),      %xmm0
	movapd	0 * SIZE(AO2),      %xmm1
	movapd	2 * SIZE(AO1),      %xmm2
	movapd	2 * SIZE(AO2),      %xmm3

	movapd	 %xmm0, %xmm4
	unpcklpd %xmm1, %xmm0
	movapd	 %xmm2, %xmm6
	unpcklpd %xmm3, %xmm2

	unpckhpd %xmm1, %xmm4
	unpckhpd %xmm3, %xmm6

#ifdef PREFETCHW
	PREFETCHW	(PREFETCHSIZE * 4 +  0) * SIZE(B)
#endif

	movapd	%xmm0,  -16 * SIZE(B)
	movapd	%xmm4,  -14 * SIZE(B)
	movapd	%xmm2,  -12 * SIZE(B)
	movapd	%xmm6,  -10 * SIZE(B)

#ifdef PREFETCH
	PREFETCH	PREFETCHSIZE * 2 * SIZE(AO2)
#endif

	movapd	4 * SIZE(AO1), %xmm0
	movapd	4 * SIZE(AO2), %xmm1
	movapd	6 * SIZE(AO1), %xmm2
	movapd	6 * SIZE(AO2), %xmm3

	movapd	 %xmm0, %xmm4
	unpcklpd %xmm1, %xmm0
	movapd	 %xmm2, %xmm6
	unpcklpd %xmm3, %xmm2

	unpckhpd %xmm1, %xmm4
	unpckhpd %xmm3, %xmm6

#ifdef PREFETCHW
	PREFETCHW	(PREFETCHSIZE * 4 +  8) * SIZE(B)
#endif

	movapd	%xmm0,  -8 * SIZE(B)
	movapd	%xmm4,  -6 * SIZE(B)
	movapd	%xmm2,  -4 * SIZE(B)
	movapd	%xmm6,  -2 * SIZE(B)

	addq	$8 * SIZE, AO1
	addq	$8 * SIZE, AO2
	subq	$-16 * SIZE, B

	decq	I
	jg	.L23
	ALIGN_4

.L24:
	testq	$4, MM
	jle	.L26

	movapd	0 * SIZE(AO1),      %xmm0
	movapd	0 * SIZE(AO2),      %xmm1
	movapd	2 * SIZE(AO1),      %xmm2
	movapd	2 * SIZE(AO2),      %xmm3

	movapd	 %xmm0, %xmm4
	unpcklpd %xmm1, %xmm0
	unpckhpd %xmm1, %xmm4

	movapd	 %xmm2, %xmm6
	unpcklpd %xmm3, %xmm2
	unpckhpd %xmm3, %xmm6

	movapd	%xmm0,  -16 * SIZE(B)
	movapd	%xmm4,  -14 * SIZE(B)
	movapd	%xmm2,  -12 * SIZE(B)
	movapd	%xmm6,  -10 * SIZE(B)

	addq	$4 * SIZE, AO1
	addq	$4 * SIZE, AO2
	subq	$-8 * SIZE, B
	ALIGN_4

.L26:
	testq	$2, MM
	jle	.L28

	movapd	0 * SIZE(AO1),      %xmm0
	movapd	0 * SIZE(AO2),      %xmm1

	movapd	 %xmm0, %xmm2
	unpcklpd %xmm1, %xmm0
	unpckhpd %xmm1, %xmm2

	movapd	%xmm0,  -16 * SIZE(B)
	movapd	%xmm2,  -14 * SIZE(B)

	addq	$2 * SIZE, AO1
	addq	$2 * SIZE, AO2
	subq	$-4 * SIZE, B
	ALIGN_4

.L28:
	testq	$1, MM
	jle	.L30

	movsd	0 * SIZE(AO1),      %xmm0
	movsd	0 * SIZE(AO2),      %xmm1

	unpcklpd %xmm1, %xmm0

	movapd	%xmm0,  -16 * SIZE(B)
	subq	$-2 * SIZE, B
	ALIGN_4

.L30:
	testq	$1, N
	jle	.L999

	movq	A, AO1

	testq	$SIZE, A
	jne	.L35

	movq	MM, I
	sarq	$3, I
	jle	.L32
	ALIGN_4

.L31:
#ifdef PREFETCH
	PREFETCH	PREFETCHSIZE * 4 * SIZE(AO1)
#endif

	movapd	0 * SIZE(AO1),      %xmm0
	movapd	2 * SIZE(AO1),      %xmm1
	movapd	4 * SIZE(AO1),      %xmm2
	movapd	6 * SIZE(AO1),      %xmm3

#ifdef PREFETCHW
	PREFETCHW	(PREFETCHSIZE * 4 +  0) * SIZE(B)
#endif

	movapd	%xmm0,  -16 * SIZE(B)
	movapd	%xmm1,  -14 * SIZE(B)
	movapd	%xmm2,  -12 * SIZE(B)
	movapd	%xmm3,  -10 * SIZE(B)

	addq	$8 * SIZE, AO1
	subq	$-8 * SIZE, B

	decq	I
	jg	.L31
	ALIGN_4

.L32:
	testq	$4, MM
	jle	.L33

	movapd	0 * SIZE(AO1),      %xmm0
	movapd	2 * SIZE(AO1),      %xmm1

	movapd	%xmm0, -16 * SIZE(B)
	movapd	%xmm1, -14 * SIZE(B)

	addq	$4 * SIZE, AO1
	subq	$-4 * SIZE, B
	ALIGN_4

.L33:
	testq	$2, MM
	jle	.L34

	movapd	0 * SIZE(AO1),      %xmm0

	movapd	%xmm0,  -16 * SIZE(B)

	addq	$2 * SIZE, AO1
	subq	$-2 * SIZE, B
	ALIGN_4

.L34:
	testq	$1, MM
	jle	.L999

	movsd	0 * SIZE(AO1),      %xmm0

	movlpd	%xmm0,  -16 * SIZE(B)
	jmp	.L999
	ALIGN_4

.L35:
	movapd	-1 * SIZE(AO1),      %xmm0

	movq	MM, I
	sarq	$3, I
	jle	.L36
	ALIGN_4

.L36:
#ifdef PREFETCH
	PREFETCH	PREFETCHSIZE * 4 * SIZE(AO1)
#endif

	movapd	1 * SIZE(AO1),      %xmm1
	movapd	3 * SIZE(AO1),      %xmm2
	movapd	5 * SIZE(AO1),      %xmm3
	movapd	7 * SIZE(AO1),      %xmm4

	shufpd	$1, %xmm1, %xmm0
	shufpd	$1, %xmm2, %xmm1
	shufpd	$1, %xmm3, %xmm2
	shufpd	$1, %xmm4, %xmm3

#ifdef PREFETCHW
	PREFETCHW	(PREFETCHSIZE * 4 +  0) * SIZE(B)
#endif

	movapd	%xmm0,  -16 * SIZE(B)
	movapd	%xmm1,  -14 * SIZE(B)
	movapd	%xmm2,  -12 * SIZE(B)
	movapd	%xmm3,  -10 * SIZE(B)

	movapd	%xmm4, %xmm0

	addq	$8 * SIZE, AO1
	subq	$-8 * SIZE, B

	decq	I
	jg	.L36
	ALIGN_4

.L37:
	testq	$4, MM
	jle	.L38

	movapd	1 * SIZE(AO1),      %xmm1
	movapd	3 * SIZE(AO1),      %xmm2

	shufpd	$1, %xmm1, %xmm0
	shufpd	$1, %xmm2, %xmm1

	movapd	%xmm0,  -16 * SIZE(B)
	movapd	%xmm1,  -14 * SIZE(B)

	movapd	%xmm2, %xmm0

	addq	$4 * SIZE, AO1
	addq	$4 * SIZE, B
	ALIGN_4

.L38:
	testq	$2, MM
	jle	.L39

	movapd	1 * SIZE(AO1),      %xmm1

	shufpd	$1, %xmm1, %xmm0

	movapd	%xmm0,  -16 * SIZE(B)

	movapd	%xmm1, %xmm0

	addq	$2 * SIZE, AO1
	subq	$-2 * SIZE, B
	ALIGN_4

.L39:
	testq	$1, MM
	jle	.L999

	shufpd	$1, %xmm0, %xmm0

	movlpd	%xmm0,  -16 * SIZE(B)
	jmp	.L999
	ALIGN_4

.L50:
	movq	N,  J
	sarq	$2, J
	jle	.L60
	ALIGN_4

.L51:
	movq	A, AO1
	leaq	(A, LDA, 2), AO2
	leaq	(A, LDA, 4), A

	testq	$SIZE, A
	je	.L52

	movsd	0 * SIZE(AO1),      %xmm0
	movsd	0 * SIZE(AO1, LDA), %xmm1
	movsd	0 * SIZE(AO2),      %xmm2
	movsd	0 * SIZE(AO2, LDA), %xmm3

	unpcklpd %xmm1, %xmm0
	unpcklpd %xmm3, %xmm2

	movapd	%xmm0,  -16 * SIZE(B)
	movapd	%xmm2,  -14 * SIZE(B)

	addq	$1 * SIZE, AO1
	addq	$1 * SIZE, AO2
	subq	$-4 * SIZE, B
	ALIGN_3

.L52:
	movapd	-1 * SIZE(AO1, LDA), %xmm5
	movapd	-1 * SIZE(AO2, LDA), %xmm7

	movq	MM, I
	sarq	$3, I
	jle	.L54
	ALIGN_4

.L53:
#ifdef PREFETCH
	PREFETCH	PREFETCHSIZE * SIZE(AO1)
#endif

	movapd	0 * SIZE(AO1),      %xmm0
	movapd	1 * SIZE(AO1, LDA), %xmm1
	movapd	0 * SIZE(AO2),      %xmm2
	movapd	1 * SIZE(AO2, LDA), %xmm3

	movsd	 %xmm0, %xmm5
	movsd	 %xmm2, %xmm7
	shufpd	 $1, %xmm1, %xmm0
	shufpd	 $1, %xmm3, %xmm2

#ifdef PREFETCHW
	PREFETCHW	(PREFETCHSIZE * 4 +  0) * SIZE(B)
#endif

	movapd	%xmm5,  -16 * SIZE(B)
	movapd	%xmm7,  -14 * SIZE(B)
	movapd	%xmm0,  -12 * SIZE(B)
	movapd	%xmm2,  -10 * SIZE(B)

#ifdef PREFETCH
	PREFETCH	PREFETCHSIZE * SIZE(AO1, LDA)
#endif

	movapd	2 * SIZE(AO1),      %xmm0
	movapd	3 * SIZE(AO1, LDA), %xmm5
	movapd	2 * SIZE(AO2),      %xmm2
	movapd	3 * SIZE(AO2, LDA), %xmm7

	movsd	 %xmm0, %xmm1
	movsd	 %xmm2, %xmm3
	shufpd	 $1, %xmm5, %xmm0
	shufpd	 $1, %xmm7, %xmm2

#ifdef PREFETCHW
	PREFETCHW	(PREFETCHSIZE * 4 +  8) * SIZE(B)
#endif

	movapd	%xmm1,  -8 * SIZE(B)
	movapd	%xmm3,  -6 * SIZE(B)
	movapd	%xmm0,  -4 * SIZE(B)
	movapd	%xmm2,  -2 * SIZE(B)

#ifdef PREFETCH
	PREFETCH	PREFETCHSIZE * SIZE(AO2)
#endif

	movapd	4 * SIZE(AO1),      %xmm0
	movapd	5 * SIZE(AO1, LDA), %xmm1
	movapd	4 * SIZE(AO2),      %xmm2
	movapd	5 * SIZE(AO2, LDA), %xmm3

	movsd	 %xmm0, %xmm5
	movsd	 %xmm2, %xmm7
	shufpd	 $1, %xmm1, %xmm0
	shufpd	 $1, %xmm3, %xmm2

#ifdef PREFETCHW
	PREFETCHW	(PREFETCHSIZE * 4 + 16) * SIZE(B)
#endif

	movapd	%xmm5,    0 * SIZE(B)
	movapd	%xmm7,    2 * SIZE(B)
	movapd	%xmm0,    4 * SIZE(B)
	movapd	%xmm2,    6 * SIZE(B)

#ifdef PREFETCH
	PREFETCH	PREFETCHSIZE * SIZE(AO2, LDA)
#endif

	movapd	6 * SIZE(AO1),      %xmm0
	movapd	7 * SIZE(AO1, LDA), %xmm5
	movapd	6 * SIZE(AO2),      %xmm2
	movapd	7 * SIZE(AO2, LDA), %xmm7

	movsd	 %xmm0, %xmm1
	movsd	 %xmm2, %xmm3
	shufpd	 $1, %xmm5, %xmm0
	shufpd	 $1, %xmm7, %xmm2

#ifdef PREFETCHW
	PREFETCHW	(PREFETCHSIZE * 4 + 24) * SIZE(B)
#endif

	movapd	%xmm1,   8 * SIZE(B)
	movapd	%xmm3,  10 * SIZE(B)
	movapd	%xmm0,  12 * SIZE(B)
	movapd	%xmm2,  14 * SIZE(B)

	addq	$8 * SIZE, AO1
	addq	$8 * SIZE, AO2
	subq	$-32 * SIZE, B

	decq	I
	jg	.L53
	ALIGN_4

.L54:
	testq	$4, MM
	jle	.L56

	movapd	0 * SIZE(AO1),      %xmm0
	movapd	1 * SIZE(AO1, LDA), %xmm1
	movapd	0 * SIZE(AO2),      %xmm2
	movapd	1 * SIZE(AO2, LDA), %xmm3

	movsd	 %xmm0, %xmm5
	shufpd	 $1, %xmm1, %xmm0
	movsd	 %xmm2, %xmm7
	shufpd	 $1, %xmm3, %xmm2

	movapd	%xmm5,  -16 * SIZE(B)
	movapd	%xmm7,  -14 * SIZE(B)
	movapd	%xmm0,  -12 * SIZE(B)
	movapd	%xmm2,  -10 * SIZE(B)

	movapd	2 * SIZE(AO1),      %xmm0
	movapd	3 * SIZE(AO1, LDA), %xmm5
	movapd	2 * SIZE(AO2),      %xmm2
	movapd	3 * SIZE(AO2, LDA), %xmm7

	movsd	 %xmm0, %xmm1
	shufpd	 $1, %xmm5, %xmm0
	movsd	 %xmm2, %xmm3
	shufpd	 $1, %xmm7, %xmm2

	movapd	%xmm1,  -8 * SIZE(B)
	movapd	%xmm3,  -6 * SIZE(B)
	movapd	%xmm0,  -4 * SIZE(B)
	movapd	%xmm2,  -2 * SIZE(B)

	addq	$4 * SIZE, AO1
	addq	$4 * SIZE, AO2
	subq	$-16 * SIZE, B
	ALIGN_4

.L56:
	testq	$2, MM
	jle	.L58

	movapd	0 * SIZE(AO1),      %xmm0
	movapd	1 * SIZE(AO1, LDA), %xmm1
	movapd	0 * SIZE(AO2),      %xmm2
	movapd	1 * SIZE(AO2, LDA), %xmm3

	movsd	 %xmm0, %xmm5
	movsd	 %xmm2, %xmm7
	shufpd	 $1, %xmm1, %xmm0
	shufpd	 $1, %xmm3, %xmm2

	movapd	%xmm5,  -16 * SIZE(B)
	movapd	%xmm7,  -14 * SIZE(B)
	movapd	%xmm0,  -12 * SIZE(B)
	movapd	%xmm2,  -10 * SIZE(B)

	addq	$2 * SIZE, AO1
	addq	$2 * SIZE, AO2
	subq	$-8 * SIZE, B
	ALIGN_4

.L58:
	testq	$1, MM
	jle	.L59

	movsd	0 * SIZE(AO1),      %xmm0
	movsd	0 * SIZE(AO1, LDA), %xmm1
	movsd	0 * SIZE(AO2),      %xmm2
	movsd	0 * SIZE(AO2, LDA), %xmm3

	unpcklpd %xmm1, %xmm0
	unpcklpd %xmm3, %xmm2

	movapd	%xmm0,  -16 * SIZE(B)
	movapd	%xmm2,  -14 * SIZE(B)
	subq	$-4 * SIZE, B
	ALIGN_4

.L59:
	decq	J
	jg	.L51
	ALIGN_4

.L60:
	testq	$2, N
	jle	.L70

	movq	A, AO1
	leaq	(A, LDA), AO2
	leaq	(A, LDA, 2), A

	testq	$SIZE, A
	je	.L62

	movsd	0 * SIZE(AO1), %xmm0
	movsd	0 * SIZE(AO2), %xmm1

	unpcklpd %xmm1, %xmm0

	movapd	%xmm0,  -16 * SIZE(B)

	addq	$1 * SIZE, AO1
	addq	$1 * SIZE, AO2
	subq	$-2 * SIZE, B
	ALIGN_3

.L62:
	movapd	-1 * SIZE(AO2), %xmm5

	movq	MM, I
	sarq	$3, I
	jle	.L64
	ALIGN_4

.L63:
#ifdef PREFETCH
	PREFETCH	PREFETCHSIZE * 2 * SIZE(AO1)
#endif

	movapd	0 * SIZE(AO1), %xmm0
	movapd	1 * SIZE(AO2), %xmm1
	movapd	2 * SIZE(AO1), %xmm2
	movapd	3 * SIZE(AO2), %xmm3

	movsd	 %xmm0, %xmm5
	shufpd	 $1, %xmm1, %xmm0
	movsd	 %xmm2, %xmm1
	shufpd	 $1, %xmm3, %xmm2

#ifdef PREFETCHW
	PREFETCHW	(PREFETCHSIZE * 4 +  0) * SIZE(B)
#endif

	movapd	%xmm5,  -16 * SIZE(B)
	movapd	%xmm0,  -14 * SIZE(B)
	movapd	%xmm1,  -12 * SIZE(B)
	movapd	%xmm2,  -10 * SIZE(B)

#ifdef PREFETCH
	PREFETCH	PREFETCHSIZE * 2 * SIZE(AO2)
#endif

	movapd	4 * SIZE(AO1), %xmm0
	movapd	5 * SIZE(AO2), %xmm1
	movapd	6 * SIZE(AO1), %xmm2
	movapd	7 * SIZE(AO2), %xmm5

	movsd	 %xmm0, %xmm3
	shufpd	 $1, %xmm1, %xmm0
	movsd	 %xmm2, %xmm1
	shufpd	 $1, %xmm5, %xmm2

#ifdef PREFETCHW
	PREFETCHW	(PREFETCHSIZE * 4 +  0) * SIZE(B)
#endif

	movapd	%xmm3,   -8 * SIZE(B)
	movapd	%xmm0,   -6 * SIZE(B)
	movapd	%xmm1,   -4 * SIZE(B)
	movapd	%xmm2,   -2 * SIZE(B)

	addq	$8 * SIZE, AO1
	addq	$8 * SIZE, AO2
	subq	$-16 * SIZE, B

	decq	I
	jg	.L63
	ALIGN_4

.L64:
	testq	$4, MM
	jle	.L66

	movapd	0 * SIZE(AO1), %xmm0
	movapd	1 * SIZE(AO2), %xmm1
	movapd	2 * SIZE(AO1), %xmm2
	movapd	3 * SIZE(AO2), %xmm3

	movsd	 %xmm0, %xmm5
	shufpd	 $1, %xmm1, %xmm0
	movsd	 %xmm2, %xmm1
	shufpd	 $1, %xmm3, %xmm2

	movapd	%xmm5,  -16 * SIZE(B)
	movapd	%xmm0,  -14 * SIZE(B)
	movapd	%xmm1,  -12 * SIZE(B)
	movapd	%xmm2,  -10 * SIZE(B)

	movaps	%xmm3, %xmm5

	addq	$4 * SIZE, AO1
	addq	$4 * SIZE, AO2
	subq	$-8 * SIZE, B
	ALIGN_4

.L66:
	testq	$2, MM
	jle	.L68

	movapd	0 * SIZE(AO1), %xmm0
	movapd	1 * SIZE(AO2), %xmm1

	movsd	 %xmm0, %xmm5
	shufpd	 $1, %xmm1, %xmm0

	movapd	%xmm5,  -16 * SIZE(B)
	movapd	%xmm0,  -14 * SIZE(B)

	addq	$2 * SIZE, AO1
	addq	$2 * SIZE, AO2
	subq	$-4 * SIZE, B
	ALIGN_4

.L68:
	testq	$1, MM
	jle	.L70

	movsd	0 * SIZE(AO1),      %xmm0
	movsd	0 * SIZE(AO2),      %xmm1

	unpcklpd %xmm1, %xmm0

	movapd	%xmm0,  -16 * SIZE(B)
	subq	$-2 * SIZE, B
	ALIGN_4

.L70:
	testq	$1, N
	jle	.L999

	movq	A, AO1

	testq	$SIZE, A
	jne	.L75

	movq	MM, I
	sarq	$3, I
	jle	.L72
	ALIGN_4

.L71:
#ifdef PREFETCH
	PREFETCH	PREFETCHSIZE * 4 * SIZE(AO1)
#endif

	movapd	0 * SIZE(AO1),      %xmm0
	movapd	2 * SIZE(AO1),      %xmm2
	movapd	4 * SIZE(AO1),      %xmm4
	movapd	6 * SIZE(AO1),      %xmm6

#ifdef PREFETCHW
	PREFETCHW	(PREFETCHSIZE * 4 +  0) * SIZE(B)
#endif

	movapd	%xmm0,  -16 * SIZE(B)
	movapd	%xmm2,  -14 * SIZE(B)
	movapd	%xmm4,  -12 * SIZE(B)
	movapd	%xmm6,  -10 * SIZE(B)

	addq	$8 * SIZE, AO1
	subq	$-8 * SIZE, B

	decq	I
	jg	.L71
	ALIGN_4

.L72:
	testq	$4, MM
	jle	.L73

	movapd	0 * SIZE(AO1),      %xmm0
	movapd	2 * SIZE(AO1),      %xmm2

	movapd	%xmm0, -16 * SIZE(B)
	movapd	%xmm2, -14 * SIZE(B)

	addq	$4 * SIZE, AO1
	subq	$-4 * SIZE, B
	ALIGN_4

.L73:
	testq	$2, MM
	jle	.L74

	movapd	0 * SIZE(AO1),      %xmm0

	movapd	%xmm0,  -16 * SIZE(B)

	addq	$2 * SIZE, AO1
	subq	$-2 * SIZE, B
	ALIGN_4

.L74:
	testq	$1, MM
	jle	.L999

	movsd	0 * SIZE(AO1),      %xmm0

	movlpd	%xmm0,  -16 * SIZE(B)
	jmp	.L999
	ALIGN_4

.L75:
	movapd	-1 * SIZE(AO1),      %xmm0

	movq	MM, I
	sarq	$3, I
	jle	.L76
	ALIGN_4

.L76:
#ifdef PREFETCH
	PREFETCH	PREFETCHSIZE * 4 * SIZE(AO1)
#endif

	movapd	1 * SIZE(AO1),      %xmm1
	movapd	3 * SIZE(AO1),      %xmm2
	movapd	5 * SIZE(AO1),      %xmm3
	movapd	7 * SIZE(AO1),      %xmm4

	shufpd	$1, %xmm1, %xmm0
	shufpd	$1, %xmm2, %xmm1
	shufpd	$1, %xmm3, %xmm2
	shufpd	$1, %xmm4, %xmm3

#ifdef PREFETCHW
	PREFETCHW	(PREFETCHSIZE * 4 +  0) * SIZE(B)
#endif

	movapd	%xmm0,  -16 * SIZE(B)
	movapd	%xmm1,  -14 * SIZE(B)
	movapd	%xmm2,  -12 * SIZE(B)
	movapd	%xmm3,  -10 * SIZE(B)

	movapd	%xmm4, %xmm0

	addq	$8 * SIZE, AO1
	subq	$-8 * SIZE, B

	decq	I
	jg	.L76
	ALIGN_4

.L77:
	testq	$4, MM
	jle	.L78

	movapd	1 * SIZE(AO1),      %xmm1
	movapd	3 * SIZE(AO1),      %xmm2

	shufpd	$1, %xmm1, %xmm0
	shufpd	$1, %xmm2, %xmm1

	movapd	%xmm0,  -16 * SIZE(B)
	movapd	%xmm1,  -14 * SIZE(B)

	movapd	%xmm2, %xmm0

	addq	$4 * SIZE, AO1
	addq	$4 * SIZE, B
	ALIGN_4

.L78:
	testq	$2, MM
	jle	.L79

	movapd	1 * SIZE(AO1),      %xmm1

	shufpd	$1, %xmm1, %xmm0

	movapd	%xmm0,  -16 * SIZE(B)

	movapd	%xmm1, %xmm0

	addq	$2 * SIZE, AO1
	subq	$-2 * SIZE, B
	ALIGN_4

.L79:
	testq	$1, MM
	jle	.L999

	shufpd	$1, %xmm0, %xmm0

	movlpd	%xmm0,  -16 * SIZE(B)
	ALIGN_4

.L999:
#ifdef WINDOWS_ABI
	movups	  0(%rsp), %xmm6
	movups	 16(%rsp), %xmm7

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
