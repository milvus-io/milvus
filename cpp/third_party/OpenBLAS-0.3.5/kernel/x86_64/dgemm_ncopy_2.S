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
#endif

#ifdef SANDYBRIDGE
#define PREFETCHSIZE	16
#define PREFETCH	prefetcht0
#define PREFETCHW	prefetcht0
#endif

#ifndef MOVAPS
#define MOVAPS	movaps
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
	sarq	$1, J
	jle	.L30
	ALIGN_4

.L21:
	movq	A, AO1
	leaq	(A, LDA), AO2
	leaq	(A, LDA, 2), A

	testq	$SIZE, A
	je	.L22

	movsd	0 * SIZE(AO1),      %xmm0
	movsd	0 * SIZE(AO2),      %xmm1

	unpcklpd %xmm1, %xmm0

	movaps	%xmm0,  -16 * SIZE(B)

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

	MOVAPS	0 * SIZE(AO1),      %xmm0
	MOVAPS	0 * SIZE(AO2),      %xmm1
	MOVAPS	2 * SIZE(AO1),      %xmm2
	MOVAPS	2 * SIZE(AO2),      %xmm3

	movaps	 %xmm0, %xmm4
	unpcklpd %xmm1, %xmm0
	movaps	 %xmm2, %xmm6
	unpcklpd %xmm3, %xmm2

	unpckhpd %xmm1, %xmm4
	unpckhpd %xmm3, %xmm6

#ifdef PREFETCHW
	PREFETCHW	(PREFETCHSIZE * 4 +  0) * SIZE(B)
#endif

	movaps	%xmm0,  -16 * SIZE(B)
	movaps	%xmm4,  -14 * SIZE(B)
	movaps	%xmm2,  -12 * SIZE(B)
	movaps	%xmm6,  -10 * SIZE(B)

#ifdef PREFETCH
	PREFETCH	PREFETCHSIZE * 2 * SIZE(AO2)
#endif

	MOVAPS	4 * SIZE(AO1), %xmm0
	MOVAPS	4 * SIZE(AO2), %xmm1
	MOVAPS	6 * SIZE(AO1), %xmm2
	MOVAPS	6 * SIZE(AO2), %xmm3

	movaps	 %xmm0, %xmm4
	unpcklpd %xmm1, %xmm0
	unpckhpd %xmm1, %xmm4

	movaps	 %xmm2, %xmm6
	unpcklpd %xmm3, %xmm2
	unpckhpd %xmm3, %xmm6

#ifdef PREFETCHW
	PREFETCHW	(PREFETCHSIZE * 4 +  8) * SIZE(B)
#endif

	movaps	%xmm0,  -8 * SIZE(B)
	movaps	%xmm4,  -6 * SIZE(B)
	movaps	%xmm2,  -4 * SIZE(B)
	movaps	%xmm6,  -2 * SIZE(B)

	addq	$8 * SIZE, AO1
	addq	$8 * SIZE, AO2
	subq	$-16 * SIZE, B

	decq	I
	jg	.L23
	ALIGN_4

.L24:
	testq	$4, MM
	jle	.L26

	MOVAPS	0 * SIZE(AO1),      %xmm0
	MOVAPS	0 * SIZE(AO2),      %xmm1
	MOVAPS	2 * SIZE(AO1),      %xmm2
	MOVAPS	2 * SIZE(AO2),      %xmm3

	movaps	 %xmm0, %xmm4
	unpcklpd %xmm1, %xmm0
	unpckhpd %xmm1, %xmm4

	movaps	 %xmm2, %xmm6
	unpcklpd %xmm3, %xmm2
	unpckhpd %xmm3, %xmm6

	movaps	%xmm0,  -16 * SIZE(B)
	movaps	%xmm4,  -14 * SIZE(B)
	movaps	%xmm2,  -12 * SIZE(B)
	movaps	%xmm6,  -10 * SIZE(B)

	addq	$4 * SIZE, AO1
	addq	$4 * SIZE, AO2
	subq	$-8 * SIZE, B
	ALIGN_4

.L26:
	testq	$2, MM
	jle	.L28

	MOVAPS	0 * SIZE(AO1),      %xmm0
	MOVAPS	0 * SIZE(AO2),      %xmm1

	movaps	 %xmm0, %xmm2
	unpcklpd %xmm1, %xmm0
	unpckhpd %xmm1, %xmm2

	movaps	%xmm0,  -16 * SIZE(B)
	movaps	%xmm2,  -14 * SIZE(B)

	addq	$2 * SIZE, AO1
	addq	$2 * SIZE, AO2
	subq	$-4 * SIZE, B
	ALIGN_4

.L28:
	testq	$1, MM
	jle	.L29

	movsd	0 * SIZE(AO1),      %xmm0
	movsd	0 * SIZE(AO2),      %xmm1

	unpcklpd %xmm1, %xmm0

	movaps	%xmm0,  -16 * SIZE(B)
	subq	$-2 * SIZE, B
	ALIGN_4

.L29:
	decq	J
	jg	.L21
	ALIGN_4

.L30:
	testq	$1, N
	jle	.L999

.L30x:
	movq	A, AO1

	testq	$SIZE, A
	jne	.L35

	movq	M,  I
	sarq	$3, I
	jle	.L32
	ALIGN_4

.L31:
#ifdef PREFETCH
	PREFETCH	PREFETCHSIZE * 4 * SIZE(AO1)
#endif

	MOVAPS	0 * SIZE(AO1),      %xmm0
	MOVAPS	2 * SIZE(AO1),      %xmm1
	MOVAPS	4 * SIZE(AO1),      %xmm2
	MOVAPS	6 * SIZE(AO1),      %xmm3

#ifdef PREFETCHW
	PREFETCHW	(PREFETCHSIZE * 4 +  0) * SIZE(B)
#endif

	movaps	%xmm0,  -16 * SIZE(B)
	movaps	%xmm1,  -14 * SIZE(B)
	movaps	%xmm2,  -12 * SIZE(B)
	movaps	%xmm3,  -10 * SIZE(B)

	addq	$8 * SIZE, AO1
	addq	$8 * SIZE, B

	decq	I
	jg	.L31
	ALIGN_4

.L32:
	testq	$4, M
	jle	.L33

	MOVAPS	0 * SIZE(AO1),      %xmm0
	MOVAPS	2 * SIZE(AO1),      %xmm1

	movaps	%xmm0, -16 * SIZE(B)
	movaps	%xmm1, -14 * SIZE(B)

	addq	$4 * SIZE, AO1
	subq	$-4 * SIZE, B
	ALIGN_4

.L33:
	testq	$2, M
	jle	.L34

	MOVAPS	0 * SIZE(AO1),      %xmm0

	movaps	%xmm0,  -16 * SIZE(B)

	addq	$2 * SIZE, AO1
	subq	$-2 * SIZE, B
	ALIGN_4

.L34:
	testq	$1, M
	jle	.L999

	movsd	0 * SIZE(AO1),      %xmm0
	movlpd	%xmm0,  -16 * SIZE(B)
	jmp	.L999
	ALIGN_4

.L35:
	movaps	-1 * SIZE(AO1),      %xmm0

	movq	M,  I
	sarq	$3, I
	jle	.L36
	ALIGN_4

.L36:
#ifdef PREFETCH
	PREFETCH	PREFETCHSIZE * 4 * SIZE(AO1)
#endif

	MOVAPS	1 * SIZE(AO1),      %xmm1
	MOVAPS	3 * SIZE(AO1),      %xmm2
	MOVAPS	5 * SIZE(AO1),      %xmm3
	MOVAPS	7 * SIZE(AO1),      %xmm4

	shufpd	$1, %xmm1, %xmm0
	shufpd	$1, %xmm2, %xmm1
	shufpd	$1, %xmm3, %xmm2
	shufpd	$1, %xmm4, %xmm3

#ifdef PREFETCHW
	PREFETCHW	(PREFETCHSIZE * 4 +  0) * SIZE(B)
#endif

	movaps	%xmm0,  -16 * SIZE(B)
	movaps	%xmm1,  -14 * SIZE(B)
	movaps	%xmm2,  -12 * SIZE(B)
	movaps	%xmm3,  -10 * SIZE(B)

	movaps	%xmm4, %xmm0

	addq	$8 * SIZE, AO1
	subq	$-8 * SIZE, B

	decq	I
	jg	.L36
	ALIGN_4

.L37:
	testq	$4, M
	jle	.L38

	MOVAPS	1 * SIZE(AO1),      %xmm1
	MOVAPS	3 * SIZE(AO1),      %xmm2

	shufpd	$1, %xmm1, %xmm0
	shufpd	$1, %xmm2, %xmm1

	movaps	%xmm0,  -16 * SIZE(B)
	movaps	%xmm1,  -14 * SIZE(B)

	movaps	%xmm2, %xmm0

	addq	$4 * SIZE, AO1
	addq	$4 * SIZE, B
	ALIGN_4

.L38:
	testq	$2, M
	jle	.L39

	MOVAPS	1 * SIZE(AO1),      %xmm1

	shufpd	$1, %xmm1, %xmm0

	movaps	%xmm0,  -16 * SIZE(B)

	movaps	%xmm1, %xmm0

	addq	$2 * SIZE, AO1
	subq	$-2 * SIZE, B
	ALIGN_4

.L39:
	testq	$1, M
	jle	.L999

	movhpd	%xmm0,  -16 * SIZE(B)
	jmp	.L999
	ALIGN_4

.L50:
	movq	N,  J
	sarq	$1, J
	jle	.L30
	ALIGN_4

.L61:
	movq	A, AO1
	leaq	(A, LDA), AO2
	leaq	(A, LDA, 2), A

	testq	$SIZE, A
	je	.L62

	movsd	0 * SIZE(AO1), %xmm0
	movsd	0 * SIZE(AO2), %xmm1

	unpcklpd %xmm1, %xmm0

	movaps	%xmm0,  -16 * SIZE(B)

	addq	$1 * SIZE, AO1
	addq	$1 * SIZE, AO2
	subq	$-2 * SIZE, B
	ALIGN_3

.L62:
	MOVAPS	-1 * SIZE(AO2), %xmm5

	movq	MM, I
	sarq	$3, I
	jle	.L64
	ALIGN_4

.L63:
#ifdef PREFETCH
	PREFETCH	PREFETCHSIZE * 2 * SIZE(AO1)
#endif

	MOVAPS	0 * SIZE(AO1), %xmm0
	MOVAPS	1 * SIZE(AO2), %xmm1
	MOVAPS	2 * SIZE(AO1), %xmm2
	MOVAPS	3 * SIZE(AO2), %xmm3

	movsd	 %xmm0, %xmm5
	shufpd	 $1, %xmm1, %xmm0
	movsd	 %xmm2, %xmm1
	shufpd	 $1, %xmm3, %xmm2

#ifdef PREFETCHW
	PREFETCHW	(PREFETCHSIZE * 4 +  0) * SIZE(B)
#endif

	movaps	%xmm5,  -16 * SIZE(B)
	movaps	%xmm0,  -14 * SIZE(B)
	movaps	%xmm1,  -12 * SIZE(B)
	movaps	%xmm2,  -10 * SIZE(B)

#ifdef PREFETCH
	PREFETCH	PREFETCHSIZE * 2 * SIZE(AO2)
#endif

	MOVAPS	4 * SIZE(AO1), %xmm0
	MOVAPS	5 * SIZE(AO2), %xmm1
	MOVAPS	6 * SIZE(AO1), %xmm2
	MOVAPS	7 * SIZE(AO2), %xmm5

	movsd	 %xmm0, %xmm3
	shufpd	 $1, %xmm1, %xmm0
	movsd	 %xmm2, %xmm1
	shufpd	 $1, %xmm5, %xmm2

#ifdef PREFETCHW
	PREFETCHW	(PREFETCHSIZE * 4 +  0) * SIZE(B)
#endif

	movaps	%xmm3,   -8 * SIZE(B)
	movaps	%xmm0,   -6 * SIZE(B)
	movaps	%xmm1,   -4 * SIZE(B)
	movaps	%xmm2,   -2 * SIZE(B)

	addq	$8 * SIZE, AO1
	addq	$8 * SIZE, AO2
	subq	$-16 * SIZE, B

	decq	I
	jg	.L63
	ALIGN_4

.L64:
	testq	$4, MM
	jle	.L66

	MOVAPS	0 * SIZE(AO1), %xmm0
	MOVAPS	1 * SIZE(AO2), %xmm1
	MOVAPS	2 * SIZE(AO1), %xmm2
	MOVAPS	3 * SIZE(AO2), %xmm3

	movsd	 %xmm0, %xmm5
	shufpd	 $1, %xmm1, %xmm0
	movsd	 %xmm2, %xmm1
	shufpd	 $1, %xmm3, %xmm2

	movaps	%xmm5,  -16 * SIZE(B)
	movaps	%xmm0,  -14 * SIZE(B)
	movaps	%xmm1,  -12 * SIZE(B)
	movaps	%xmm2,  -10 * SIZE(B)

	movaps	%xmm3, %xmm5

	addq	$4 * SIZE, AO1
	addq	$4 * SIZE, AO2
	subq	$-8 * SIZE, B
	ALIGN_4

.L66:
	testq	$2, MM
	jle	.L68

	MOVAPS	0 * SIZE(AO1), %xmm0
	MOVAPS	1 * SIZE(AO2), %xmm1

	movsd	 %xmm0, %xmm5
	shufpd	 $1, %xmm1, %xmm0

	movaps	%xmm5,  -16 * SIZE(B)
	movaps	%xmm0,  -14 * SIZE(B)

	addq	$2 * SIZE, AO1
	addq	$2 * SIZE, AO2
	subq	$-4 * SIZE, B
	ALIGN_4

.L68:
	testq	$1, MM
	jle	.L69

	movsd	0 * SIZE(AO1),      %xmm0
	movsd	0 * SIZE(AO2),      %xmm1

	unpcklpd %xmm1, %xmm0

	movaps	%xmm0,  -16 * SIZE(B)
	subq	$-2 * SIZE, B
	ALIGN_4

.L69:
	decq	J
	jg	.L61

	testq	$1, N
	jne	.L30
	ALIGN_4

.L999:
#ifdef WINDOWS_ABI
	movups	  0(%rsp), %xmm6

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
