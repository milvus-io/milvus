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

	leaq	(,LDA, SIZE), LDA		# Scaling

	movq	N,  J
	sarq	$1, J
	jle	.L20
	ALIGN_4

.L01:
	movq	A, AO1
	leaq	(A,   LDA), AO2
	leaq	(A, LDA, 2), A

	movq	M,  I
	sarq	$3, I
	jle	.L08
	ALIGN_4

.L03:

#ifndef DOUBLE
	vmovss	0 * SIZE(AO1), %xmm0
	vmovss	0 * SIZE(AO2), %xmm1
	vmovss	1 * SIZE(AO1), %xmm2
	vmovss	1 * SIZE(AO2), %xmm3
	vmovss	2 * SIZE(AO1), %xmm4
	vmovss	2 * SIZE(AO2), %xmm5
	vmovss	3 * SIZE(AO1), %xmm6
	vmovss	3 * SIZE(AO2), %xmm7

	vmovss	4 * SIZE(AO1), %xmm8
	vmovss	4 * SIZE(AO2), %xmm9
	vmovss	5 * SIZE(AO1), %xmm10
	vmovss	5 * SIZE(AO2), %xmm11
	vmovss	6 * SIZE(AO1), %xmm12
	vmovss	6 * SIZE(AO2), %xmm13
	vmovss	7 * SIZE(AO1), %xmm14
	vmovss	7 * SIZE(AO2), %xmm15

	vmovss	%xmm0,   0 * SIZE(B)
	vmovss	%xmm1,   1 * SIZE(B)
	vmovss	%xmm2,   2 * SIZE(B)
	vmovss	%xmm3,   3 * SIZE(B)
	vmovss	%xmm4,   4 * SIZE(B)
	vmovss	%xmm5,   5 * SIZE(B)
	vmovss	%xmm6,   6 * SIZE(B)
	vmovss	%xmm7,   7 * SIZE(B)

	vmovss	%xmm8,   8 * SIZE(B)
	vmovss	%xmm9,   9 * SIZE(B)
	vmovss	%xmm10, 10 * SIZE(B)
	vmovss	%xmm11, 11 * SIZE(B)
	vmovss	%xmm12, 12 * SIZE(B)
	vmovss	%xmm13, 13 * SIZE(B)
	vmovss	%xmm14, 14 * SIZE(B)
	vmovss	%xmm15, 15 * SIZE(B)

#else
	prefetchw	256(B)

	prefetchnta	256(AO1)
	vmovsd	0 * SIZE(AO1), %xmm0
	vmovsd	1 * SIZE(AO1), %xmm1
	vmovsd	2 * SIZE(AO1), %xmm2
	vmovsd	3 * SIZE(AO1), %xmm3
	vmovsd	4 * SIZE(AO1), %xmm4
	vmovsd	5 * SIZE(AO1), %xmm5
	vmovsd	6 * SIZE(AO1), %xmm6
	vmovsd	7 * SIZE(AO1), %xmm7

	prefetchnta	256(AO2)
	vmovhpd	0 * SIZE(AO2), %xmm0 , %xmm0
	vmovhpd	1 * SIZE(AO2), %xmm1 , %xmm1
	vmovhpd	2 * SIZE(AO2), %xmm2 , %xmm2
	vmovhpd	3 * SIZE(AO2), %xmm3 , %xmm3
	vmovhpd	4 * SIZE(AO2), %xmm4 , %xmm4
	vmovhpd	5 * SIZE(AO2), %xmm5 , %xmm5
	vmovhpd	6 * SIZE(AO2), %xmm6 , %xmm6
	vmovhpd	7 * SIZE(AO2), %xmm7 , %xmm7


	prefetchw	256+64(B)
	vmovups	%xmm0,   0 * SIZE(B)
	vmovups	%xmm1,   2 * SIZE(B)
	vmovups	%xmm2,   4 * SIZE(B)
	vmovups	%xmm3,   6 * SIZE(B)
	vmovups	%xmm4,   8 * SIZE(B)
	vmovups	%xmm5,  10 * SIZE(B)
	vmovups	%xmm6,  12 * SIZE(B)
	vmovups	%xmm7,  14 * SIZE(B)

#endif

	addq	$8 * SIZE, AO1
	addq	$8 * SIZE, AO2
	subq	$-16 * SIZE, B
	decq	I
	jg	.L03
	ALIGN_4


.L08:
	testq	$4 , M
	je	.L14

	ALIGN_4


.L13:
#ifndef DOUBLE
	vmovss	0 * SIZE(AO1), %xmm0
	vmovss	0 * SIZE(AO2), %xmm1
	vmovss	1 * SIZE(AO1), %xmm2
	vmovss	1 * SIZE(AO2), %xmm3
	vmovss	2 * SIZE(AO1), %xmm4
	vmovss	2 * SIZE(AO2), %xmm5
	vmovss	3 * SIZE(AO1), %xmm6
	vmovss	3 * SIZE(AO2), %xmm7

	vmovss	%xmm0,   0 * SIZE(B)
	vmovss	%xmm1,   1 * SIZE(B)
	vmovss	%xmm2,   2 * SIZE(B)
	vmovss	%xmm3,   3 * SIZE(B)
	vmovss	%xmm4,   4 * SIZE(B)
	vmovss	%xmm5,   5 * SIZE(B)
	vmovss	%xmm6,   6 * SIZE(B)
	vmovss	%xmm7,   7 * SIZE(B)
#else

	vmovsd	0 * SIZE(AO1), %xmm0
	vmovsd	1 * SIZE(AO1), %xmm1
	vmovsd	2 * SIZE(AO1), %xmm2
	vmovsd	3 * SIZE(AO1), %xmm3

	vmovhpd	0 * SIZE(AO2), %xmm0 , %xmm0
	vmovhpd	1 * SIZE(AO2), %xmm1 , %xmm1
	vmovhpd	2 * SIZE(AO2), %xmm2 , %xmm2
	vmovhpd	3 * SIZE(AO2), %xmm3 , %xmm3


	vmovups	%xmm0,   0 * SIZE(B)
	vmovups	%xmm1,   2 * SIZE(B)
	vmovups	%xmm2,   4 * SIZE(B)
	vmovups	%xmm3,   6 * SIZE(B)
#endif

	addq	$4 * SIZE, AO1
	addq	$4 * SIZE, AO2
	subq	$-8 * SIZE, B
	ALIGN_4

.L14:
	movq	M,  I
	andq	$3, I
	jle	.L16
	ALIGN_4

.L15:
#ifndef DOUBLE
	vmovss	0 * SIZE(AO1), %xmm0
	vmovss	0 * SIZE(AO2), %xmm1

	vmovss	%xmm0,   0 * SIZE(B)
	vmovss	%xmm1,   1 * SIZE(B)
#else
	vmovsd	0 * SIZE(AO1), %xmm0
	vmovhpd	0 * SIZE(AO2), %xmm0 , %xmm0

	vmovups	%xmm0,   0 * SIZE(B)
#endif

	addq	$SIZE, AO1
	addq	$SIZE, AO2
	addq	$2 * SIZE, B
	decq	I
	jg	.L15
	ALIGN_4

.L16:
	decq	J
	jg	.L01
	ALIGN_4

.L20:
	testq	$1, N
	jle	.L999

	movq	A, AO1

	movq	M,  I
	sarq	$2, I
	jle	.L34
	ALIGN_4

.L33:
#ifndef DOUBLE
	vmovups	0 * SIZE(AO1), %xmm0

	vmovups	%xmm0,   0 * SIZE(B)
#else
	vmovups	0 * SIZE(AO1), %xmm0
	vmovups	2 * SIZE(AO1), %xmm1

	vmovups	%xmm0,   0 * SIZE(B)
	vmovups	%xmm1,   2 * SIZE(B)
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
	vmovss	0 * SIZE(AO1), %xmm0
	vmovss	%xmm0,   0 * SIZE(B)
#else
	vmovsd	0 * SIZE(AO1), %xmm0
	vmovsd	%xmm0,   0 * SIZE(B)
#endif

	addq	$SIZE, AO1
	addq	$1 * SIZE, B
	decq	I
	jg	.L35
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

	popq	%r12
	popq	%r13

#ifdef WINDOWS_ABI
	popq	%r14
	popq	%r15
#endif
	ret

	EPILOGUE
