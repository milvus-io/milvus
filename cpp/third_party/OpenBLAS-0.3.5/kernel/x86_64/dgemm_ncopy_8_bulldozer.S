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

#define A_PRE 256
#define B_PRE 128


#ifndef WINDOWS_ABI

#define M	ARG1	/* rdi */
#define N	ARG2	/* rsi */
#define A	ARG3	/* rdx */
#define LDA	ARG4	/* rcx */
#define B	ARG5	/* r8  */

#define AO1	%r9
#define AO2	%r10
#define LDA3	%r11
#define J	%r12
#define MM	%r13

#else

#define STACKSIZE 128

#define M	ARG1	/* rcx */
#define N	ARG2	/* rdx */
#define A	ARG3	/* r8  */
#define LDA	ARG4	/* r9  */
#define OLD_B		40 + 32 + STACKSIZE(%rsp)

#define B	%r15

#define AO1	%r10
#define AO2	%r11
#define LDA3	%r12
#define J	%r13
#define MM	%r14

#endif

#define I	%rax

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

	movq	OLD_B,     B
#endif

	leaq	(,LDA, SIZE), LDA
	leaq	(LDA, LDA, 2), LDA3
	subq	$-16 * SIZE, B

	movq	M, MM
	leaq	-1(M), %rax
	testq	$SIZE, A
	cmovne	%rax, MM

	testq	$SIZE, LDA
	jne	.L50

	movq	N,  J
	sarq	$3, J
	jle	.L20
	ALIGN_4

.L11:
	movq	A, AO1
	leaq	(A, LDA, 4), AO2
	leaq	(A, LDA, 8), A

	testq	$SIZE, A
	je	.L12

	vmovsd	0 * SIZE(AO1),         %xmm0
	vmovsd	0 * SIZE(AO1, LDA),    %xmm1
	vmovsd	0 * SIZE(AO1, LDA, 2), %xmm2
	vmovsd	0 * SIZE(AO1, LDA3),   %xmm3

	vmovsd	0 * SIZE(AO2),         %xmm4
	vmovsd	0 * SIZE(AO2, LDA),    %xmm5
	vmovsd	0 * SIZE(AO2, LDA, 2), %xmm6
	vmovsd	0 * SIZE(AO2, LDA3),   %xmm7

	vunpcklpd %xmm1, %xmm0 , %xmm0
	vunpcklpd %xmm3, %xmm2 , %xmm2
	vunpcklpd %xmm5, %xmm4 , %xmm4
	vunpcklpd %xmm7, %xmm6 , %xmm6

	vmovups	%xmm0,  -16 * SIZE(B)
	vmovups	%xmm2,  -14 * SIZE(B)
	vmovups	%xmm4,  -12 * SIZE(B)
	vmovups	%xmm6,  -10 * SIZE(B)

	addq	$1 * SIZE, AO1
	addq	$1 * SIZE, AO2
	subq	$-8 * SIZE, B
	ALIGN_3

.L12:
	movq	MM, I
	sarq	$3, I
	jle	.L14
	ALIGN_4

.L13:

	prefetchnta	A_PRE(AO1)
	vmovups	0 * SIZE(AO1),         %xmm0
	vmovups	0 * SIZE(AO1, LDA),    %xmm1
	prefetchnta	A_PRE(AO1, LDA)
	vmovups	0 * SIZE(AO1, LDA, 2), %xmm2
	vmovups	0 * SIZE(AO1, LDA3),   %xmm3

	prefetchnta	A_PRE(AO1, LDA, 2)
	vmovups	0 * SIZE(AO1, LDA, 2), %xmm2
	vmovups	 %xmm0, %xmm8
	prefetchnta	A_PRE(AO1, LDA3)
	vunpcklpd %xmm1, %xmm0 , %xmm0
	vmovups	 %xmm2, %xmm9
	vunpcklpd %xmm3, %xmm2 , %xmm2


	prefetchnta	A_PRE(AO2)
	vmovups	0 * SIZE(AO2),         %xmm4
	vmovups	0 * SIZE(AO2, LDA),    %xmm5
	prefetchnta	A_PRE(AO2, LDA)
	vmovups	0 * SIZE(AO2, LDA, 2), %xmm6
	vmovups	0 * SIZE(AO2, LDA3),   %xmm7

	prefetchnta	A_PRE(AO2, LDA, 2)
	vmovups	 %xmm4, %xmm10
	vunpcklpd %xmm5, %xmm4 , %xmm4
	prefetchnta	A_PRE(AO2, LDA3)
	vmovups	 %xmm6, %xmm11
	vunpcklpd %xmm7, %xmm6 , %xmm6

	prefetchw	B_PRE(B)
	vmovups	%xmm0,  -16 * SIZE(B)
	vmovups	%xmm2,  -14 * SIZE(B)
	vmovups	%xmm4,  -12 * SIZE(B)
	vmovups	%xmm6,  -10 * SIZE(B)

	vunpckhpd %xmm1, %xmm8 , %xmm8
	vunpckhpd %xmm3, %xmm9 , %xmm9
	vunpckhpd %xmm5, %xmm10, %xmm10
	vunpckhpd %xmm7, %xmm11, %xmm11


	prefetchw	B_PRE+64(B)
	vmovups	%xmm8,   -8 * SIZE(B)
	vmovups	%xmm9,   -6 * SIZE(B)
	vmovups	%xmm10,  -4 * SIZE(B)
	vmovups	%xmm11,  -2 * SIZE(B)

/***********************************************************************************************/

	vmovups	2 * SIZE(AO1),         %xmm0
	vmovups	2 * SIZE(AO1, LDA),    %xmm1
	vmovups	2 * SIZE(AO1, LDA, 2), %xmm2
	vmovups	2 * SIZE(AO1, LDA3),   %xmm3

	vmovups	 %xmm0, %xmm8
	vunpcklpd %xmm1, %xmm0 , %xmm0
	vmovups	 %xmm2, %xmm9
	vunpcklpd %xmm3, %xmm2 , %xmm2


	vmovups	2 * SIZE(AO2),         %xmm4
	vmovups	2 * SIZE(AO2, LDA),    %xmm5
	vmovups	2 * SIZE(AO2, LDA, 2), %xmm6
	vmovups	2 * SIZE(AO2, LDA3),   %xmm7

	vmovups	 %xmm4, %xmm10
	vunpcklpd %xmm5, %xmm4 , %xmm4
	vmovups	 %xmm6, %xmm11
	vunpcklpd %xmm7, %xmm6 , %xmm6


	prefetchw	B_PRE+128(B)
	vmovups	%xmm0,    0 * SIZE(B)
	vmovups	%xmm2,    2 * SIZE(B)
	vmovups	%xmm4,    4 * SIZE(B)
	vmovups	%xmm6,    6 * SIZE(B)

	vunpckhpd %xmm1, %xmm8 , %xmm8
	vunpckhpd %xmm3, %xmm9 , %xmm9
	vunpckhpd %xmm5, %xmm10, %xmm10
	vunpckhpd %xmm7, %xmm11, %xmm11


	prefetchw	B_PRE+192(B)
	vmovups	%xmm8,    8 * SIZE(B)
	vmovups	%xmm9,   10 * SIZE(B)
	vmovups	%xmm10,  12 * SIZE(B)
	vmovups	%xmm11,  14 * SIZE(B)

/***********************************************************************************************/

	vmovups	4 * SIZE(AO1),         %xmm0
	vmovups	4 * SIZE(AO1, LDA),    %xmm1
	vmovups	4 * SIZE(AO1, LDA, 2), %xmm2
	vmovups	4 * SIZE(AO1, LDA3),   %xmm3

	vmovups	 %xmm0, %xmm8
	vunpcklpd %xmm1, %xmm0 , %xmm0
	vmovups	 %xmm2, %xmm9
	vunpcklpd %xmm3, %xmm2 , %xmm2


	vmovups	4 * SIZE(AO2),         %xmm4
	vmovups	4 * SIZE(AO2, LDA),    %xmm5
	vmovups	4 * SIZE(AO2, LDA, 2), %xmm6
	vmovups	4 * SIZE(AO2, LDA3),   %xmm7

	vmovups	 %xmm4, %xmm10
	vunpcklpd %xmm5, %xmm4 , %xmm4
	vmovups	 %xmm6, %xmm11
	vunpcklpd %xmm7, %xmm6 , %xmm6


	prefetchw	B_PRE+256(B)
	vmovups	%xmm0,   16 * SIZE(B)
	vmovups	%xmm2,   18 * SIZE(B)
	vmovups	%xmm4,   20 * SIZE(B)
	vmovups	%xmm6,   22 * SIZE(B)

	vunpckhpd %xmm1, %xmm8 , %xmm8
	vunpckhpd %xmm3, %xmm9 , %xmm9
	vunpckhpd %xmm5, %xmm10, %xmm10
	vunpckhpd %xmm7, %xmm11, %xmm11

	prefetchw	B_PRE+320(B)
	vmovups	%xmm8,   24 * SIZE(B)
	vmovups	%xmm9,   26 * SIZE(B)
	vmovups	%xmm10,  28 * SIZE(B)
	vmovups	%xmm11,  30 * SIZE(B)

/***********************************************************************************************/

	vmovups	6 * SIZE(AO1),         %xmm0
	vmovups	6 * SIZE(AO1, LDA),    %xmm1
	vmovups	6 * SIZE(AO1, LDA, 2), %xmm2
	vmovups	6 * SIZE(AO1, LDA3),   %xmm3

	vmovups	 %xmm0, %xmm8
	vunpcklpd %xmm1, %xmm0 , %xmm0
	vmovups	 %xmm2, %xmm9
	vunpcklpd %xmm3, %xmm2 , %xmm2


	vmovups	6 * SIZE(AO2),         %xmm4
	vmovups	6 * SIZE(AO2, LDA),    %xmm5
	vmovups	6 * SIZE(AO2, LDA, 2), %xmm6
	vmovups	6 * SIZE(AO2, LDA3),   %xmm7

	vmovups	 %xmm4, %xmm10
	vunpcklpd %xmm5, %xmm4 , %xmm4
	vmovups	 %xmm6, %xmm11
	vunpcklpd %xmm7, %xmm6 , %xmm6


	prefetchw	B_PRE+384(B)
	vmovups	%xmm0,   32 * SIZE(B)
	vmovups	%xmm2,   34 * SIZE(B)
	vmovups	%xmm4,   36 * SIZE(B)
	vmovups	%xmm6,   38 * SIZE(B)

	vunpckhpd %xmm1, %xmm8 , %xmm8
	vunpckhpd %xmm3, %xmm9 , %xmm9
	vunpckhpd %xmm5, %xmm10, %xmm10
	vunpckhpd %xmm7, %xmm11, %xmm11

	prefetchw	B_PRE+448(B)
	vmovups	%xmm8,   40 * SIZE(B)
	vmovups	%xmm9,   42 * SIZE(B)
	vmovups	%xmm10,  44 * SIZE(B)
	vmovups	%xmm11,  46 * SIZE(B)

	addq	$8 * SIZE, AO1
	addq	$8 * SIZE, AO2
	subq	$-64 * SIZE, B

	decq	I
	jg	.L13
	ALIGN_4

.L14:
	testq	$4, MM
	jle	.L16

	vmovups	0 * SIZE(AO1),         %xmm0
	vmovups	0 * SIZE(AO1, LDA),    %xmm1
	vmovups	0 * SIZE(AO1, LDA, 2), %xmm2
	vmovups	0 * SIZE(AO1, LDA3),   %xmm3

	vmovups	0 * SIZE(AO2),         %xmm4
	vmovups	0 * SIZE(AO2, LDA),    %xmm5
	vmovups	0 * SIZE(AO2, LDA, 2), %xmm6
	vmovups	0 * SIZE(AO2, LDA3),   %xmm7

	vmovups	 %xmm0, %xmm8
	vunpcklpd %xmm1, %xmm0 , %xmm0
	vmovups	 %xmm2, %xmm9
	vunpcklpd %xmm3, %xmm2 , %xmm2

	vmovups	 %xmm4, %xmm10
	vunpcklpd %xmm5, %xmm4 , %xmm4
	vmovups	 %xmm6, %xmm11
	vunpcklpd %xmm7, %xmm6 , %xmm6

	vmovups	%xmm0,  -16 * SIZE(B)
	vmovups	%xmm2,  -14 * SIZE(B)
	vmovups	%xmm4,  -12 * SIZE(B)
	vmovups	%xmm6,  -10 * SIZE(B)

	vunpckhpd %xmm1, %xmm8 , %xmm8
	vunpckhpd %xmm3, %xmm9 , %xmm9
	vunpckhpd %xmm5, %xmm10, %xmm10
	vunpckhpd %xmm7, %xmm11, %xmm11

	vmovups	%xmm8,   -8 * SIZE(B)
	vmovups	%xmm9,   -6 * SIZE(B)
	vmovups	%xmm10,  -4 * SIZE(B)
	vmovups	%xmm11,  -2 * SIZE(B)

	vmovups	2 * SIZE(AO1),         %xmm0
	vmovups	2 * SIZE(AO1, LDA),    %xmm1
	vmovups	2 * SIZE(AO1, LDA, 2), %xmm2
	vmovups	2 * SIZE(AO1, LDA3),   %xmm3

	vmovups	2 * SIZE(AO2),         %xmm4
	vmovups	2 * SIZE(AO2, LDA),    %xmm5
	vmovups	2 * SIZE(AO2, LDA, 2), %xmm6
	vmovups	2 * SIZE(AO2, LDA3),   %xmm7

	vmovups	 %xmm0, %xmm8
	vunpcklpd %xmm1, %xmm0 , %xmm0
	vmovups	 %xmm2, %xmm9
	vunpcklpd %xmm3, %xmm2 , %xmm2

	vmovups	 %xmm4, %xmm10
	vunpcklpd %xmm5, %xmm4 , %xmm4
	vmovups	 %xmm6, %xmm11
	vunpcklpd %xmm7, %xmm6 , %xmm6

	vmovups	%xmm0,    0 * SIZE(B)
	vmovups	%xmm2,    2 * SIZE(B)
	vmovups	%xmm4,    4 * SIZE(B)
	vmovups	%xmm6,    6 * SIZE(B)

	vunpckhpd %xmm1, %xmm8 , %xmm8
	vunpckhpd %xmm3, %xmm9 , %xmm9
	vunpckhpd %xmm5, %xmm10, %xmm10
	vunpckhpd %xmm7, %xmm11, %xmm11

	vmovups	%xmm8,    8 * SIZE(B)
	vmovups	%xmm9,   10 * SIZE(B)
	vmovups	%xmm10,  12 * SIZE(B)
	vmovups	%xmm11,  14 * SIZE(B)

	addq	$4 * SIZE, AO1
	addq	$4 * SIZE, AO2
	subq	$-32 * SIZE, B
	ALIGN_4

.L16:
	testq	$2, MM
	jle	.L18

	vmovups	0 * SIZE(AO1),         %xmm0
	vmovups	0 * SIZE(AO1, LDA),    %xmm1
	vmovups	0 * SIZE(AO1, LDA, 2), %xmm2
	vmovups	0 * SIZE(AO1, LDA3),   %xmm3

	vmovups	0 * SIZE(AO2),         %xmm4
	vmovups	0 * SIZE(AO2, LDA),    %xmm5
	vmovups	0 * SIZE(AO2, LDA, 2), %xmm6
	vmovups	0 * SIZE(AO2, LDA3),   %xmm7

	vmovups	 %xmm0, %xmm8
	vunpcklpd %xmm1, %xmm0 , %xmm0
	vmovups	 %xmm2, %xmm9
	vunpcklpd %xmm3, %xmm2 , %xmm2

	vmovups	 %xmm4, %xmm10
	vunpcklpd %xmm5, %xmm4 , %xmm4
	vmovups	 %xmm6, %xmm11
	vunpcklpd %xmm7, %xmm6 , %xmm6

	vmovups	%xmm0,  -16 * SIZE(B)
	vmovups	%xmm2,  -14 * SIZE(B)
	vmovups	%xmm4,  -12 * SIZE(B)
	vmovups	%xmm6,  -10 * SIZE(B)

	vunpckhpd %xmm1, %xmm8 , %xmm8
	vunpckhpd %xmm3, %xmm9 , %xmm9
	vunpckhpd %xmm5, %xmm10, %xmm10
	vunpckhpd %xmm7, %xmm11, %xmm11

	vmovups	%xmm8,   -8 * SIZE(B)
	vmovups	%xmm9,   -6 * SIZE(B)
	vmovups	%xmm10,  -4 * SIZE(B)
	vmovups	%xmm11,  -2 * SIZE(B)

	addq	$2 * SIZE, AO1
	addq	$2 * SIZE, AO2
	subq	$-16 * SIZE, B
	ALIGN_4

.L18:
	testq	$1, MM
	jle	.L19

	vmovsd	0 * SIZE(AO1),         %xmm0
	vmovsd	0 * SIZE(AO1, LDA),    %xmm1
	vmovsd	0 * SIZE(AO1, LDA, 2), %xmm2
	vmovsd	0 * SIZE(AO1, LDA3),   %xmm3

	vmovsd	0 * SIZE(AO2),         %xmm4
	vmovsd	0 * SIZE(AO2, LDA),    %xmm5
	vmovsd	0 * SIZE(AO2, LDA, 2), %xmm6
	vmovsd	0 * SIZE(AO2, LDA3),   %xmm7

	vunpcklpd %xmm1, %xmm0 , %xmm0
	vunpcklpd %xmm3, %xmm2 , %xmm2
	vunpcklpd %xmm5, %xmm4 , %xmm4
	vunpcklpd %xmm7, %xmm6 , %xmm6

	vmovups	%xmm0,  -16 * SIZE(B)
	vmovups	%xmm2,  -14 * SIZE(B)
	vmovups	%xmm4,  -12 * SIZE(B)
	vmovups	%xmm6,  -10 * SIZE(B)

	subq	$-8 * SIZE, B
	ALIGN_4

.L19:
	decq	J
	jg	.L11
	ALIGN_4

.L20:
	testq	$4, N
	jle	.L30

	movq	A, AO1
	leaq	(A, LDA, 2), AO2
	leaq	(A, LDA, 4), A

	testq	$SIZE, A
	je	.L22

	vmovsd	0 * SIZE(AO1),      %xmm0
	vmovsd	0 * SIZE(AO1, LDA), %xmm1
	vmovsd	0 * SIZE(AO2),      %xmm2
	vmovsd	0 * SIZE(AO2, LDA), %xmm3

	vunpcklpd %xmm1, %xmm0 , %xmm0
	vunpcklpd %xmm3, %xmm2 , %xmm2

	vmovups	%xmm0,  -16 * SIZE(B)
	vmovups	%xmm2,  -14 * SIZE(B)

	addq	$1 * SIZE, AO1
	addq	$1 * SIZE, AO2
	subq	$-4 * SIZE, B
	ALIGN_3

.L22:
	movq	MM, I
	sarq	$3, I
	jle	.L24
	ALIGN_4

.L23:

	vmovups	0 * SIZE(AO1),      %xmm0
	vmovups	0 * SIZE(AO1, LDA), %xmm1
	vmovups	0 * SIZE(AO2),      %xmm2
	vmovups	0 * SIZE(AO2, LDA), %xmm3

	vmovups	 %xmm0, %xmm4
	vunpcklpd %xmm1, %xmm0 , %xmm0
	vmovups	 %xmm2, %xmm6
	vunpcklpd %xmm3, %xmm2 , %xmm2

	vunpckhpd %xmm1, %xmm4 , %xmm4
	vunpckhpd %xmm3, %xmm6 , %xmm6


	vmovups	%xmm0,  -16 * SIZE(B)
	vmovups	%xmm2,  -14 * SIZE(B)
	vmovups	%xmm4,  -12 * SIZE(B)
	vmovups	%xmm6,  -10 * SIZE(B)


	vmovups	2 * SIZE(AO1),      %xmm0
	vmovups	2 * SIZE(AO1, LDA), %xmm1
	vmovups	2 * SIZE(AO2),      %xmm2
	vmovups	2 * SIZE(AO2, LDA), %xmm3

	vmovups	 %xmm0, %xmm4
	vunpcklpd %xmm1, %xmm0 , %xmm0
	vmovups	 %xmm2, %xmm6
	vunpcklpd %xmm3, %xmm2 , %xmm2

	vunpckhpd %xmm1, %xmm4 , %xmm4
	vunpckhpd %xmm3, %xmm6 , %xmm6


	vmovups	%xmm0,  -8 * SIZE(B)
	vmovups	%xmm2,  -6 * SIZE(B)
	vmovups	%xmm4,  -4 * SIZE(B)
	vmovups	%xmm6,  -2 * SIZE(B)


	vmovups	4 * SIZE(AO1),      %xmm0
	vmovups	4 * SIZE(AO1, LDA), %xmm1
	vmovups	4 * SIZE(AO2),      %xmm2
	vmovups	4 * SIZE(AO2, LDA), %xmm3

	vmovups	 %xmm0, %xmm4
	vunpcklpd %xmm1, %xmm0 , %xmm0
	vmovups	 %xmm2, %xmm6
	vunpcklpd %xmm3, %xmm2 , %xmm2

	vunpckhpd %xmm1, %xmm4 , %xmm4
	vunpckhpd %xmm3, %xmm6 , %xmm6


	vmovups	%xmm0,   0 * SIZE(B)
	vmovups	%xmm2,   2 * SIZE(B)
	vmovups	%xmm4,   4 * SIZE(B)
	vmovups	%xmm6,   6 * SIZE(B)


	vmovups	6 * SIZE(AO1),      %xmm0
	vmovups	6 * SIZE(AO1, LDA), %xmm1
	vmovups	6 * SIZE(AO2),      %xmm2
	vmovups	6 * SIZE(AO2, LDA), %xmm3

	vmovups	 %xmm0, %xmm4
	vunpcklpd %xmm1, %xmm0 , %xmm0
	vmovups	 %xmm2, %xmm6
	vunpcklpd %xmm3, %xmm2 , %xmm2

	vunpckhpd %xmm1, %xmm4 , %xmm4
	vunpckhpd %xmm3, %xmm6 , %xmm6


	vmovups	%xmm0,   8 * SIZE(B)
	vmovups	%xmm2,  10 * SIZE(B)
	vmovups	%xmm4,  12 * SIZE(B)
	vmovups	%xmm6,  14 * SIZE(B)

	addq	$8 * SIZE, AO1
	addq	$8 * SIZE, AO2
	subq	$-32 * SIZE, B

	decq	I
	jg	.L23
	ALIGN_4

.L24:
	testq	$4, MM
	jle	.L26

	vmovups	0 * SIZE(AO1),      %xmm0
	vmovups	0 * SIZE(AO1, LDA), %xmm1
	vmovups	0 * SIZE(AO2),      %xmm2
	vmovups	0 * SIZE(AO2, LDA), %xmm3

	vmovups	 %xmm0, %xmm4
	vunpcklpd %xmm1, %xmm0 , %xmm0
	vmovups	 %xmm2, %xmm6
	vunpcklpd %xmm3, %xmm2 , %xmm2

	vunpckhpd %xmm1, %xmm4 , %xmm4
	vunpckhpd %xmm3, %xmm6 , %xmm6

	vmovups	%xmm0,  -16 * SIZE(B)
	vmovups	%xmm2,  -14 * SIZE(B)
	vmovups	%xmm4,  -12 * SIZE(B)
	vmovups	%xmm6,  -10 * SIZE(B)

	vmovups	2 * SIZE(AO1),      %xmm0
	vmovups	2 * SIZE(AO1, LDA), %xmm1
	vmovups	2 * SIZE(AO2),      %xmm2
	vmovups	2 * SIZE(AO2, LDA), %xmm3

	vmovups	 %xmm0, %xmm4
	vunpcklpd %xmm1, %xmm0 , %xmm0
	vmovups	 %xmm2, %xmm6
	vunpcklpd %xmm3, %xmm2 , %xmm2

	vunpckhpd %xmm1, %xmm4 , %xmm4
	vunpckhpd %xmm3, %xmm6 , %xmm6

	vmovups	%xmm0,  -8 * SIZE(B)
	vmovups	%xmm2,  -6 * SIZE(B)
	vmovups	%xmm4,  -4 * SIZE(B)
	vmovups	%xmm6,  -2 * SIZE(B)

	addq	$4 * SIZE, AO1
	addq	$4 * SIZE, AO2
	subq	$-16 * SIZE, B
	ALIGN_4

.L26:
	testq	$2, MM
	jle	.L28

	vmovups	0 * SIZE(AO1),      %xmm0
	vmovups	0 * SIZE(AO1, LDA), %xmm1
	vmovups	0 * SIZE(AO2),      %xmm2
	vmovups	0 * SIZE(AO2, LDA), %xmm3

	vmovups	 %xmm0, %xmm4
	vunpcklpd %xmm1, %xmm0 , %xmm0
	vmovups	 %xmm2, %xmm6
	vunpcklpd %xmm3, %xmm2 , %xmm2

	vunpckhpd %xmm1, %xmm4 , %xmm4
	vunpckhpd %xmm3, %xmm6 , %xmm6

	vmovups	%xmm0,  -16 * SIZE(B)
	vmovups	%xmm2,  -14 * SIZE(B)
	vmovups	%xmm4,  -12 * SIZE(B)
	vmovups	%xmm6,  -10 * SIZE(B)

	addq	$2 * SIZE, AO1
	addq	$2 * SIZE, AO2
	subq	$-8 * SIZE, B
	ALIGN_4

.L28:
	testq	$1, MM
	jle	.L30

	vmovsd	0 * SIZE(AO1),      %xmm0
	vmovsd	0 * SIZE(AO1, LDA), %xmm1
	vmovsd	0 * SIZE(AO2),      %xmm2
	vmovsd	0 * SIZE(AO2, LDA), %xmm3

	vunpcklpd %xmm1, %xmm0 , %xmm0
	vunpcklpd %xmm3, %xmm2 , %xmm2

	vmovups	%xmm0,  -16 * SIZE(B)
	vmovups	%xmm2,  -14 * SIZE(B)
	subq	$-4 * SIZE, B
	ALIGN_4

.L30:
	testq	$2, N
	jle	.L40

	movq	A, AO1
	leaq	(A, LDA), AO2
	leaq	(A, LDA, 2), A

	testq	$SIZE, A
	je	.L32

	vmovsd	0 * SIZE(AO1),      %xmm0
	vmovsd	0 * SIZE(AO2),      %xmm1

	vunpcklpd %xmm1, %xmm0 , %xmm0

	vmovups	%xmm0,  -16 * SIZE(B)

	addq	$1 * SIZE, AO1
	addq	$1 * SIZE, AO2
	subq	$-2 * SIZE, B
	ALIGN_3

.L32:
	movq	MM, I
	sarq	$3, I
	jle	.L34
	ALIGN_4

.L33:

	vmovups	0 * SIZE(AO1),      %xmm0
	vmovups	0 * SIZE(AO2),      %xmm1
	vmovups	2 * SIZE(AO1),      %xmm2
	vmovups	2 * SIZE(AO2),      %xmm3

	vmovups	 %xmm0, %xmm4
	vunpcklpd %xmm1, %xmm0 , %xmm0
	vmovups	 %xmm2, %xmm6
	vunpcklpd %xmm3, %xmm2 , %xmm2

	vunpckhpd %xmm1, %xmm4 , %xmm4
	vunpckhpd %xmm3, %xmm6 , %xmm6


	vmovups	%xmm0,  -16 * SIZE(B)
	vmovups	%xmm4,  -14 * SIZE(B)
	vmovups	%xmm2,  -12 * SIZE(B)
	vmovups	%xmm6,  -10 * SIZE(B)


	vmovups	4 * SIZE(AO1), %xmm0
	vmovups	4 * SIZE(AO2), %xmm1
	vmovups	6 * SIZE(AO1), %xmm2
	vmovups	6 * SIZE(AO2), %xmm3

	vmovups	 %xmm0, %xmm4
	vunpcklpd %xmm1, %xmm0 , %xmm0
	vmovups	 %xmm2, %xmm6
	vunpcklpd %xmm3, %xmm2 , %xmm2

	vunpckhpd %xmm1, %xmm4 , %xmm4
	vunpckhpd %xmm3, %xmm6 , %xmm6


	vmovups	%xmm0,  -8 * SIZE(B)
	vmovups	%xmm4,  -6 * SIZE(B)
	vmovups	%xmm2,  -4 * SIZE(B)
	vmovups	%xmm6,  -2 * SIZE(B)

	addq	$8 * SIZE, AO1
	addq	$8 * SIZE, AO2
	subq	$-16 * SIZE, B

	decq	I
	jg	.L33
	ALIGN_4

.L34:
	testq	$4, MM
	jle	.L36

	vmovups	0 * SIZE(AO1),      %xmm0
	vmovups	0 * SIZE(AO2),      %xmm1
	vmovups	2 * SIZE(AO1),      %xmm2
	vmovups	2 * SIZE(AO2),      %xmm3

	vmovups	 %xmm0, %xmm4
	vunpcklpd %xmm1, %xmm0 , %xmm0
	vunpckhpd %xmm1, %xmm4 , %xmm4

	vmovups	 %xmm2, %xmm6
	vunpcklpd %xmm3, %xmm2 , %xmm2
	vunpckhpd %xmm3, %xmm6 , %xmm6

	vmovups	%xmm0,  -16 * SIZE(B)
	vmovups	%xmm4,  -14 * SIZE(B)
	vmovups	%xmm2,  -12 * SIZE(B)
	vmovups	%xmm6,  -10 * SIZE(B)

	addq	$4 * SIZE, AO1
	addq	$4 * SIZE, AO2
	subq	$-8 * SIZE, B
	ALIGN_4

.L36:
	testq	$2, MM
	jle	.L38

	vmovups	0 * SIZE(AO1),      %xmm0
	vmovups	0 * SIZE(AO2),      %xmm1

	vmovups	 %xmm0, %xmm2
	vunpcklpd %xmm1, %xmm0 , %xmm0
	vunpckhpd %xmm1, %xmm2 , %xmm2

	vmovups	%xmm0,  -16 * SIZE(B)
	vmovups	%xmm2,  -14 * SIZE(B)

	addq	$2 * SIZE, AO1
	addq	$2 * SIZE, AO2
	subq	$-4 * SIZE, B
	ALIGN_4

.L38:
	testq	$1, MM
	jle	.L40

	vmovsd	0 * SIZE(AO1),      %xmm0
	vmovsd	0 * SIZE(AO2),      %xmm1

	vunpcklpd %xmm1, %xmm0 , %xmm0

	vmovups	%xmm0,  -16 * SIZE(B)
	subq	$-2 * SIZE, B
	ALIGN_4

.L40:
	testq	$1, N
	jle	.L999

	movq	A, AO1

	testq	$SIZE, A
	jne	.L45

	movq	MM, I
	sarq	$3, I
	jle	.L42
	ALIGN_4

.L41:

	vmovups	0 * SIZE(AO1),      %xmm0
	vmovups	2 * SIZE(AO1),      %xmm1
	vmovups	4 * SIZE(AO1),      %xmm2
	vmovups	6 * SIZE(AO1),      %xmm3


	vmovups	%xmm0,  -16 * SIZE(B)
	vmovups	%xmm1,  -14 * SIZE(B)
	vmovups	%xmm2,  -12 * SIZE(B)
	vmovups	%xmm3,  -10 * SIZE(B)

	addq	$8 * SIZE, AO1
	subq	$-8 * SIZE, B

	decq	I
	jg	.L41
	ALIGN_4

.L42:
	testq	$4, MM
	jle	.L43

	vmovups	0 * SIZE(AO1),      %xmm0
	vmovups	2 * SIZE(AO1),      %xmm1

	vmovups	%xmm0, -16 * SIZE(B)
	vmovups	%xmm1, -14 * SIZE(B)

	addq	$4 * SIZE, AO1
	subq	$-4 * SIZE, B
	ALIGN_4

.L43:
	testq	$2, MM
	jle	.L44

	vmovups	0 * SIZE(AO1),      %xmm0

	vmovups	%xmm0,  -16 * SIZE(B)

	addq	$2 * SIZE, AO1
	subq	$-2 * SIZE, B
	ALIGN_4

.L44:
	testq	$1, MM
	jle	.L999

	vmovsd	0 * SIZE(AO1),      %xmm0

	vmovlpd	%xmm0,  -16 * SIZE(B)
	jmp	.L999
	ALIGN_4

.L45:
	vmovups	-1 * SIZE(AO1),      %xmm0

	movq	M, I
	sarq	$3, I
	jle	.L46
	ALIGN_4

.L46:

	vmovups	1 * SIZE(AO1),      %xmm1
	vmovups	3 * SIZE(AO1),      %xmm2
	vmovups	5 * SIZE(AO1),      %xmm3
	vmovups	7 * SIZE(AO1),      %xmm4

	vshufpd	$1, %xmm1, %xmm0 , %xmm0
	vshufpd	$1, %xmm2, %xmm1 , %xmm1
	vshufpd	$1, %xmm3, %xmm2 , %xmm2
	vshufpd	$1, %xmm4, %xmm3 , %xmm3


	vmovups	%xmm0,  -16 * SIZE(B)
	vmovups	%xmm1,  -14 * SIZE(B)
	vmovups	%xmm2,  -12 * SIZE(B)
	vmovups	%xmm3,  -10 * SIZE(B)

	vmovups	%xmm4, %xmm0

	addq	$8 * SIZE, AO1
	subq	$-8 * SIZE, B

	decq	I
	jg	.L46
	ALIGN_4

.L47:
	testq	$4, M
	jle	.L48

	vmovups	1 * SIZE(AO1),      %xmm1
	vmovups	3 * SIZE(AO1),      %xmm2

	vshufpd	$1, %xmm1, %xmm0 , %xmm0
	vshufpd	$1, %xmm2, %xmm1 , %xmm1

	vmovups	%xmm0,  -16 * SIZE(B)
	vmovups	%xmm1,  -14 * SIZE(B)

	vmovups	%xmm2, %xmm0

	addq	$4 * SIZE, AO1
	addq	$4 * SIZE, B
	ALIGN_4

.L48:
	testq	$2, M
	jle	.L49

	vmovups	1 * SIZE(AO1),      %xmm1

	vshufpd	$1, %xmm1, %xmm0 , %xmm0

	vmovups	%xmm0,  -16 * SIZE(B)

	vmovups	%xmm1, %xmm0

	addq	$2 * SIZE, AO1
	subq	$-2 * SIZE, B
	ALIGN_4

.L49:
	testq	$1, M
	jle	.L999

	vshufpd	$1, %xmm0, %xmm0 , %xmm0

	vmovlpd	%xmm0,  -16 * SIZE(B)
	jmp	.L999
	ALIGN_4

.L50:
	movq	N,  J
	sarq	$3, J
	jle	.L60
	ALIGN_4

.L51:
	movq	A, AO1
	leaq	(A, LDA, 4), AO2
	leaq	(A, LDA, 8), A

	testq	$SIZE, A
	je	.L52

	vmovsd	0 * SIZE(AO1),         %xmm0
	vmovsd	0 * SIZE(AO1, LDA),    %xmm1
	vmovsd	0 * SIZE(AO1, LDA, 2), %xmm2
	vmovsd	0 * SIZE(AO1, LDA3),   %xmm3
	vmovsd	0 * SIZE(AO2),         %xmm4
	vmovsd	0 * SIZE(AO2, LDA),    %xmm5
	vmovsd	0 * SIZE(AO2, LDA, 2), %xmm6
	vmovsd	0 * SIZE(AO2, LDA3),   %xmm7

	vunpcklpd %xmm1, %xmm0 , %xmm0
	vunpcklpd %xmm3, %xmm2 , %xmm2
	vunpcklpd %xmm5, %xmm4 , %xmm4
	vunpcklpd %xmm7, %xmm6 , %xmm6

	vmovups	%xmm0,  -16 * SIZE(B)
	vmovups	%xmm2,  -14 * SIZE(B)
	vmovups	%xmm4,  -12 * SIZE(B)
	vmovups	%xmm6,  -10 * SIZE(B)

	addq	$1 * SIZE, AO1
	addq	$1 * SIZE, AO2
	subq	$-8 * SIZE, B
	ALIGN_3

.L52:
	vmovups	-1 * SIZE(AO1, LDA),  %xmm9
	vmovups	-1 * SIZE(AO1, LDA3), %xmm10
	vmovups	-1 * SIZE(AO2, LDA),  %xmm11
	vmovups	-1 * SIZE(AO2, LDA3), %xmm12

	movq	MM, I
	sarq	$3, I
	jle	.L54
	ALIGN_4

.L53:

	vmovups	0 * SIZE(AO1),         %xmm0
	vmovups	1 * SIZE(AO1, LDA),    %xmm1
	vmovups	0 * SIZE(AO1, LDA, 2), %xmm2
	vmovups	1 * SIZE(AO1, LDA3),   %xmm3


	vmovups	0 * SIZE(AO2),         %xmm4
	vmovups	1 * SIZE(AO2, LDA),    %xmm5
	vmovups	0 * SIZE(AO2, LDA, 2), %xmm6
	vmovups	1 * SIZE(AO2, LDA3),   %xmm7

	vmovsd %xmm0, %xmm9 , %xmm9
	vmovsd %xmm2, %xmm10, %xmm10
	vmovsd %xmm4, %xmm11, %xmm11
	vmovsd %xmm6, %xmm12, %xmm12


	vmovups	%xmm9,  -16 * SIZE(B)
	vmovups	%xmm10, -14 * SIZE(B)
	vmovups	%xmm11, -12 * SIZE(B)
	vmovups	%xmm12, -10 * SIZE(B)

	vshufpd	 $1, %xmm1, %xmm0 , %xmm0
	vshufpd	 $1, %xmm3, %xmm2 , %xmm2
	vshufpd	 $1, %xmm5, %xmm4 , %xmm4
	vshufpd	 $1, %xmm7, %xmm6 , %xmm6


	vmovups	%xmm0,   -8 * SIZE(B)
	vmovups	%xmm2,   -6 * SIZE(B)
	vmovups	%xmm4,   -4 * SIZE(B)
	vmovups	%xmm6,   -2 * SIZE(B)


	vmovups	2 * SIZE(AO1),         %xmm0
	vmovups	3 * SIZE(AO1, LDA),    %xmm9
	vmovups	2 * SIZE(AO1, LDA, 2), %xmm2
	vmovups	3 * SIZE(AO1, LDA3),   %xmm10


	vmovups	2 * SIZE(AO2),         %xmm4
	vmovups	3 * SIZE(AO2, LDA),    %xmm11
	vmovups	2 * SIZE(AO2, LDA, 2), %xmm6
	vmovups	3 * SIZE(AO2, LDA3),   %xmm12

	vmovsd	 %xmm0, %xmm1 , %xmm1
	vmovsd	 %xmm2, %xmm3 , %xmm3
	vmovsd	 %xmm4, %xmm5 , %xmm5
	vmovsd	 %xmm6, %xmm7 , %xmm7


	vmovups	%xmm1,    0 * SIZE(B)
	vmovups	%xmm3,    2 * SIZE(B)
	vmovups	%xmm5,    4 * SIZE(B)
	vmovups	%xmm7,    6 * SIZE(B)

	vshufpd	 $1, %xmm9,  %xmm0 , %xmm0
	vshufpd	 $1, %xmm10, %xmm2 , %xmm2
	vshufpd	 $1, %xmm11, %xmm4 , %xmm4
	vshufpd	 $1, %xmm12, %xmm6 , %xmm6


	vmovups	%xmm0,    8 * SIZE(B)
	vmovups	%xmm2,   10 * SIZE(B)
	vmovups	%xmm4,   12 * SIZE(B)
	vmovups	%xmm6,   14 * SIZE(B)


	vmovups	4 * SIZE(AO1),         %xmm0
	vmovups	5 * SIZE(AO1, LDA),    %xmm1
	vmovups	4 * SIZE(AO1, LDA, 2), %xmm2
	vmovups	5 * SIZE(AO1, LDA3),   %xmm3


	vmovups	4 * SIZE(AO2),         %xmm4
	vmovups	5 * SIZE(AO2, LDA),    %xmm5
	vmovups	4 * SIZE(AO2, LDA, 2), %xmm6
	vmovups	5 * SIZE(AO2, LDA3),   %xmm7

	vmovsd	 %xmm0, %xmm9 , %xmm9
	vmovsd	 %xmm2, %xmm10, %xmm10
	vmovsd	 %xmm4, %xmm11, %xmm11
	vmovsd	 %xmm6, %xmm12, %xmm12


	vmovups	%xmm9,   16 * SIZE(B)
	vmovups	%xmm10,  18 * SIZE(B)
	vmovups	%xmm11,  20 * SIZE(B)
	vmovups	%xmm12,  22 * SIZE(B)

	vshufpd	 $1, %xmm1, %xmm0 , %xmm0
	vshufpd	 $1, %xmm3, %xmm2 , %xmm2
	vshufpd	 $1, %xmm5, %xmm4 , %xmm4
	vshufpd	 $1, %xmm7, %xmm6 , %xmm6


	vmovups	%xmm0,   24 * SIZE(B)
	vmovups	%xmm2,   26 * SIZE(B)
	vmovups	%xmm4,   28 * SIZE(B)
	vmovups	%xmm6,   30 * SIZE(B)


	vmovups	6 * SIZE(AO1),         %xmm0
	vmovups	7 * SIZE(AO1, LDA),    %xmm9
	vmovups	6 * SIZE(AO1, LDA, 2), %xmm2
	vmovups	7 * SIZE(AO1, LDA3),   %xmm10


	vmovups	6 * SIZE(AO2),         %xmm4
	vmovups	7 * SIZE(AO2, LDA),    %xmm11
	vmovups	6 * SIZE(AO2, LDA, 2), %xmm6
	vmovups	7 * SIZE(AO2, LDA3),   %xmm12

	vmovsd	 %xmm0, %xmm1 , %xmm1
	vmovsd	 %xmm2, %xmm3 , %xmm3
	vmovsd	 %xmm4, %xmm5 , %xmm5
	vmovsd	 %xmm6, %xmm7 , %xmm7


	vmovups	%xmm1,   32 * SIZE(B)
	vmovups	%xmm3,   34 * SIZE(B)
	vmovups	%xmm5,   36 * SIZE(B)
	vmovups	%xmm7,   38 * SIZE(B)

	vshufpd	 $1, %xmm9,  %xmm0 , %xmm0
	vshufpd	 $1, %xmm10, %xmm2 , %xmm2
	vshufpd	 $1, %xmm11, %xmm4 , %xmm4
	vshufpd	 $1, %xmm12, %xmm6 , %xmm6

	vmovups	%xmm0,   40 * SIZE(B)
	vmovups	%xmm2,   42 * SIZE(B)
	vmovups	%xmm4,   44 * SIZE(B)
	vmovups	%xmm6,   46 * SIZE(B)

	addq	$8 * SIZE, AO1
	addq	$8 * SIZE, AO2
	subq	$-64 * SIZE, B

	decq	I
	jg	.L53
	ALIGN_4

.L54:
	testq	$4, MM
	jle	.L56

	vmovups	0 * SIZE(AO1),         %xmm0
	vmovups	1 * SIZE(AO1, LDA),    %xmm1
	vmovups	0 * SIZE(AO1, LDA, 2), %xmm2
	vmovups	1 * SIZE(AO1, LDA3),   %xmm3
	vmovups	0 * SIZE(AO2),         %xmm4
	vmovups	1 * SIZE(AO2, LDA),    %xmm5
	vmovups	0 * SIZE(AO2, LDA, 2), %xmm6
	vmovups	1 * SIZE(AO2, LDA3),   %xmm7

	vmovsd	 %xmm0, %xmm9 , %xmm9
	vmovsd	 %xmm2, %xmm10, %xmm10
	vmovsd	 %xmm4, %xmm11, %xmm11
	vmovsd	 %xmm6, %xmm12, %xmm12

	vmovups	%xmm9,  -16 * SIZE(B)
	vmovups	%xmm10, -14 * SIZE(B)
	vmovups	%xmm11, -12 * SIZE(B)
	vmovups	%xmm12, -10 * SIZE(B)

	vshufpd	 $1, %xmm1, %xmm0 , %xmm0
	vshufpd	 $1, %xmm3, %xmm2 , %xmm2
	vshufpd	 $1, %xmm5, %xmm4 , %xmm4
	vshufpd	 $1, %xmm7, %xmm6 , %xmm6

	vmovups	%xmm0,   -8 * SIZE(B)
	vmovups	%xmm2,   -6 * SIZE(B)
	vmovups	%xmm4,   -4 * SIZE(B)
	vmovups	%xmm6,   -2 * SIZE(B)

	vmovups	2 * SIZE(AO1),         %xmm0
	vmovups	3 * SIZE(AO1, LDA),    %xmm9
	vmovups	2 * SIZE(AO1, LDA, 2), %xmm2
	vmovups	3 * SIZE(AO1, LDA3),   %xmm10
	vmovups	2 * SIZE(AO2),         %xmm4
	vmovups	3 * SIZE(AO2, LDA),    %xmm11
	vmovups	2 * SIZE(AO2, LDA, 2), %xmm6
	vmovups	3 * SIZE(AO2, LDA3),   %xmm12

	vmovsd	 %xmm0, %xmm1 , %xmm1
	vmovsd	 %xmm2, %xmm3 , %xmm3
	vmovsd	 %xmm4, %xmm5 , %xmm5
	vmovsd	 %xmm6, %xmm7 , %xmm7

	vmovups	%xmm1,    0 * SIZE(B)
	vmovups	%xmm3,    2 * SIZE(B)
	vmovups	%xmm5,    4 * SIZE(B)
	vmovups	%xmm7,    6 * SIZE(B)

	vshufpd	 $1, %xmm9,  %xmm0 , %xmm0
	vshufpd	 $1, %xmm10, %xmm2 , %xmm2
	vshufpd	 $1, %xmm11, %xmm4 , %xmm4
	vshufpd	 $1, %xmm12, %xmm6 , %xmm6

	vmovups	%xmm0,    8 * SIZE(B)
	vmovups	%xmm2,   10 * SIZE(B)
	vmovups	%xmm4,   12 * SIZE(B)
	vmovups	%xmm6,   14 * SIZE(B)

	addq	$4 * SIZE, AO1
	addq	$4 * SIZE, AO2
	subq	$-32 * SIZE, B
	ALIGN_4

.L56:
	testq	$2, MM
	jle	.L58

	vmovups	0 * SIZE(AO1),         %xmm0
	vmovups	1 * SIZE(AO1, LDA),    %xmm1
	vmovups	0 * SIZE(AO1, LDA, 2), %xmm2
	vmovups	1 * SIZE(AO1, LDA3),   %xmm3
	vmovups	0 * SIZE(AO2),         %xmm4
	vmovups	1 * SIZE(AO2, LDA),    %xmm5
	vmovups	0 * SIZE(AO2, LDA, 2), %xmm6
	vmovups	1 * SIZE(AO2, LDA3),   %xmm7

	vmovsd	 %xmm0, %xmm9 , %xmm9
	vmovsd	 %xmm2, %xmm10, %xmm10
	vmovsd	 %xmm4, %xmm11, %xmm11
	vmovsd	 %xmm6, %xmm12, %xmm12

	vmovups	%xmm9,  -16 * SIZE(B)
	vmovups	%xmm10, -14 * SIZE(B)
	vmovups	%xmm11, -12 * SIZE(B)
	vmovups	%xmm12, -10 * SIZE(B)

	vshufpd	 $1, %xmm1, %xmm0 , %xmm0
	vshufpd	 $1, %xmm3, %xmm2 , %xmm2
	vshufpd	 $1, %xmm5, %xmm4 , %xmm4
	vshufpd	 $1, %xmm7, %xmm6 , %xmm6

	vmovups	%xmm0,   -8 * SIZE(B)
	vmovups	%xmm2,   -6 * SIZE(B)
	vmovups	%xmm4,   -4 * SIZE(B)
	vmovups	%xmm6,   -2 * SIZE(B)

	addq	$2 * SIZE, AO1
	addq	$2 * SIZE, AO2
	subq	$-16 * SIZE, B
	ALIGN_4

.L58:
	testq	$1, MM
	jle	.L59

	vmovsd	0 * SIZE(AO1),         %xmm0
	vmovsd	0 * SIZE(AO1, LDA),    %xmm1
	vmovsd	0 * SIZE(AO1, LDA, 2), %xmm2
	vmovsd	0 * SIZE(AO1, LDA3),   %xmm3
	vmovsd	0 * SIZE(AO2),         %xmm4
	vmovsd	0 * SIZE(AO2, LDA),    %xmm5
	vmovsd	0 * SIZE(AO2, LDA, 2), %xmm6
	vmovsd	0 * SIZE(AO2, LDA3),   %xmm7

	vunpcklpd %xmm1, %xmm0 , %xmm0
	vunpcklpd %xmm3, %xmm2 , %xmm2
	vunpcklpd %xmm5, %xmm4 , %xmm4
	vunpcklpd %xmm7, %xmm6 , %xmm6

	vmovups	%xmm0,  -16 * SIZE(B)
	vmovups	%xmm2,  -14 * SIZE(B)
	vmovups	%xmm4,  -12 * SIZE(B)
	vmovups	%xmm6,  -10 * SIZE(B)

	subq	$-8 * SIZE, B
	ALIGN_4

.L59:
	decq	J
	jg	.L51
	ALIGN_4

.L60:
	testq	$4, N
	jle	.L70

	movq	A, AO1
	leaq	(A, LDA, 2), AO2
	leaq	(A, LDA, 4), A

	testq	$SIZE, A
	je	.L62

	vmovsd	0 * SIZE(AO1),      %xmm0
	vmovsd	0 * SIZE(AO1, LDA), %xmm1
	vmovsd	0 * SIZE(AO2),      %xmm2
	vmovsd	0 * SIZE(AO2, LDA), %xmm3

	vunpcklpd %xmm1, %xmm0 , %xmm0
	vunpcklpd %xmm3, %xmm2 , %xmm2

	vmovups	%xmm0,  -16 * SIZE(B)
	vmovups	%xmm2,  -14 * SIZE(B)

	addq	$1 * SIZE, AO1
	addq	$1 * SIZE, AO2
	subq	$-4 * SIZE, B
	ALIGN_3

.L62:
	vmovups	-1 * SIZE(AO1, LDA), %xmm5
	vmovups	-1 * SIZE(AO2, LDA), %xmm7

	movq	MM, I
	sarq	$3, I
	jle	.L64
	ALIGN_4

.L63:

	vmovups	0 * SIZE(AO1),      %xmm0
	vmovups	1 * SIZE(AO1, LDA), %xmm1
	vmovups	0 * SIZE(AO2),      %xmm2
	vmovups	1 * SIZE(AO2, LDA), %xmm3

	vmovsd	 %xmm0, %xmm5 , %xmm5
	vmovsd	 %xmm2, %xmm7 , %xmm7
	vshufpd	 $1, %xmm1, %xmm0 , %xmm0
	vshufpd	 $1, %xmm3, %xmm2 , %xmm2


	vmovups	%xmm5,  -16 * SIZE(B)
	vmovups	%xmm7,  -14 * SIZE(B)
	vmovups	%xmm0,  -12 * SIZE(B)
	vmovups	%xmm2,  -10 * SIZE(B)


	vmovups	2 * SIZE(AO1),      %xmm0
	vmovups	3 * SIZE(AO1, LDA), %xmm5
	vmovups	2 * SIZE(AO2),      %xmm2
	vmovups	3 * SIZE(AO2, LDA), %xmm7

	vmovsd	 %xmm0, %xmm1 , %xmm1
	vmovsd	 %xmm2, %xmm3 , %xmm3
	vshufpd	 $1, %xmm5, %xmm0 , %xmm0
	vshufpd	 $1, %xmm7, %xmm2 , %xmm2


	vmovups	%xmm1,  -8 * SIZE(B)
	vmovups	%xmm3,  -6 * SIZE(B)
	vmovups	%xmm0,  -4 * SIZE(B)
	vmovups	%xmm2,  -2 * SIZE(B)


	vmovups	4 * SIZE(AO1),      %xmm0
	vmovups	5 * SIZE(AO1, LDA), %xmm1
	vmovups	4 * SIZE(AO2),      %xmm2
	vmovups	5 * SIZE(AO2, LDA), %xmm3

	vmovsd	 %xmm0, %xmm5 , %xmm5
	vmovsd	 %xmm2, %xmm7 , %xmm7
	vshufpd	 $1, %xmm1, %xmm0 , %xmm0
	vshufpd	 $1, %xmm3, %xmm2 , %xmm2


	vmovups	%xmm5,    0 * SIZE(B)
	vmovups	%xmm7,    2 * SIZE(B)
	vmovups	%xmm0,    4 * SIZE(B)
	vmovups	%xmm2,    6 * SIZE(B)


	vmovups	6 * SIZE(AO1),      %xmm0
	vmovups	7 * SIZE(AO1, LDA), %xmm5
	vmovups	6 * SIZE(AO2),      %xmm2
	vmovups	7 * SIZE(AO2, LDA), %xmm7

	vmovsd	 %xmm0, %xmm1 , %xmm1
	vmovsd	 %xmm2, %xmm3 , %xmm3
	vshufpd	 $1, %xmm5, %xmm0 , %xmm0
	vshufpd	 $1, %xmm7, %xmm2 , %xmm2


	vmovups	%xmm1,   8 * SIZE(B)
	vmovups	%xmm3,  10 * SIZE(B)
	vmovups	%xmm0,  12 * SIZE(B)
	vmovups	%xmm2,  14 * SIZE(B)

	addq	$8 * SIZE, AO1
	addq	$8 * SIZE, AO2
	subq	$-32 * SIZE, B

	decq	I
	jg	.L63
	ALIGN_4

.L64:
	testq	$4, MM
	jle	.L66

	vmovups	0 * SIZE(AO1),      %xmm0
	vmovups	1 * SIZE(AO1, LDA), %xmm1
	vmovups	0 * SIZE(AO2),      %xmm2
	vmovups	1 * SIZE(AO2, LDA), %xmm3

	vmovsd	 %xmm0, %xmm5 , %xmm5
	vshufpd	 $1, %xmm1, %xmm0 , %xmm0
	vmovsd	 %xmm2, %xmm7 , %xmm7
	vshufpd	 $1, %xmm3, %xmm2 , %xmm2

	vmovups	%xmm5,  -16 * SIZE(B)
	vmovups	%xmm7,  -14 * SIZE(B)
	vmovups	%xmm0,  -12 * SIZE(B)
	vmovups	%xmm2,  -10 * SIZE(B)

	vmovups	2 * SIZE(AO1),      %xmm0
	vmovups	3 * SIZE(AO1, LDA), %xmm5
	vmovups	2 * SIZE(AO2),      %xmm2
	vmovups	3 * SIZE(AO2, LDA), %xmm7

	vmovsd	 %xmm0, %xmm1 , %xmm1
	vshufpd	 $1, %xmm5, %xmm0 , %xmm0
	vmovsd	 %xmm2, %xmm3 , %xmm3
	vshufpd	 $1, %xmm7, %xmm2 , %xmm2

	vmovups	%xmm1,  -8 * SIZE(B)
	vmovups	%xmm3,  -6 * SIZE(B)
	vmovups	%xmm0,  -4 * SIZE(B)
	vmovups	%xmm2,  -2 * SIZE(B)

	addq	$4 * SIZE, AO1
	addq	$4 * SIZE, AO2
	subq	$-16 * SIZE, B
	ALIGN_4

.L66:
	testq	$2, MM
	jle	.L68

	vmovups	0 * SIZE(AO1),      %xmm0
	vmovups	1 * SIZE(AO1, LDA), %xmm1
	vmovups	0 * SIZE(AO2),      %xmm2
	vmovups	1 * SIZE(AO2, LDA), %xmm3

	vmovsd	 %xmm0, %xmm5 , %xmm5
	vmovsd	 %xmm2, %xmm7 , %xmm7
	vshufpd	 $1, %xmm1, %xmm0 , %xmm0
	vshufpd	 $1, %xmm3, %xmm2 , %xmm2

	vmovups	%xmm5,  -16 * SIZE(B)
	vmovups	%xmm7,  -14 * SIZE(B)
	vmovups	%xmm0,  -12 * SIZE(B)
	vmovups	%xmm2,  -10 * SIZE(B)

	addq	$2 * SIZE, AO1
	addq	$2 * SIZE, AO2
	subq	$-8 * SIZE, B
	ALIGN_4

.L68:
	testq	$1, MM
	jle	.L70

	vmovsd	0 * SIZE(AO1),      %xmm0
	vmovsd	0 * SIZE(AO1, LDA), %xmm1
	vmovsd	0 * SIZE(AO2),      %xmm2
	vmovsd	0 * SIZE(AO2, LDA), %xmm3

	vunpcklpd %xmm1, %xmm0 , %xmm0
	vunpcklpd %xmm3, %xmm2 , %xmm2

	vmovups	%xmm0,  -16 * SIZE(B)
	vmovups	%xmm2,  -14 * SIZE(B)
	subq	$-4 * SIZE, B
	ALIGN_4

.L70:
	testq	$2, N
	jle	.L80

	movq	A, AO1
	leaq	(A, LDA), AO2
	leaq	(A, LDA, 2), A

	testq	$SIZE, A
	je	.L72

	vmovsd	0 * SIZE(AO1), %xmm0
	vmovsd	0 * SIZE(AO2), %xmm1

	vunpcklpd %xmm1, %xmm0 , %xmm0

	vmovups	%xmm0,  -16 * SIZE(B)

	addq	$1 * SIZE, AO1
	addq	$1 * SIZE, AO2
	subq	$-2 * SIZE, B
	ALIGN_3

.L72:
	vmovups	-1 * SIZE(AO2), %xmm5

	movq	MM, I
	sarq	$3, I
	jle	.L74
	ALIGN_4

.L73:

	vmovups	0 * SIZE(AO1), %xmm0
	vmovups	1 * SIZE(AO2), %xmm1
	vmovups	2 * SIZE(AO1), %xmm2
	vmovups	3 * SIZE(AO2), %xmm3

	vmovsd	 %xmm0, %xmm5 , %xmm5
	vshufpd	 $1, %xmm1, %xmm0 , %xmm0
	vmovsd	 %xmm2, %xmm1 , %xmm1
	vshufpd	 $1, %xmm3, %xmm2 , %xmm2


	vmovups	%xmm5,  -16 * SIZE(B)
	vmovups	%xmm0,  -14 * SIZE(B)
	vmovups	%xmm1,  -12 * SIZE(B)
	vmovups	%xmm2,  -10 * SIZE(B)


	vmovups	4 * SIZE(AO1), %xmm0
	vmovups	5 * SIZE(AO2), %xmm1
	vmovups	6 * SIZE(AO1), %xmm2
	vmovups	7 * SIZE(AO2), %xmm5

	vmovsd	 %xmm0, %xmm3 , %xmm3
	vshufpd	 $1, %xmm1, %xmm0 , %xmm0
	vmovsd	 %xmm2, %xmm1 , %xmm1
	vshufpd	 $1, %xmm5, %xmm2 , %xmm2


	vmovups	%xmm3,   -8 * SIZE(B)
	vmovups	%xmm0,   -6 * SIZE(B)
	vmovups	%xmm1,   -4 * SIZE(B)
	vmovups	%xmm2,   -2 * SIZE(B)

	addq	$8 * SIZE, AO1
	addq	$8 * SIZE, AO2
	subq	$-16 * SIZE, B

	decq	I
	jg	.L73
	ALIGN_4

.L74:
	testq	$4, MM
	jle	.L76

	vmovups	0 * SIZE(AO1), %xmm0
	vmovups	1 * SIZE(AO2), %xmm1
	vmovups	2 * SIZE(AO1), %xmm2
	vmovups	3 * SIZE(AO2), %xmm3

	vmovsd	 %xmm0, %xmm5 , %xmm5
	vshufpd	 $1, %xmm1, %xmm0 , %xmm0
	vmovsd	 %xmm2, %xmm1 , %xmm1
	vshufpd	 $1, %xmm3, %xmm2 , %xmm2

	vmovups	%xmm5,  -16 * SIZE(B)
	vmovups	%xmm0,  -14 * SIZE(B)
	vmovups	%xmm1,  -12 * SIZE(B)
	vmovups	%xmm2,  -10 * SIZE(B)

	vmovups	%xmm3, %xmm5

	addq	$4 * SIZE, AO1
	addq	$4 * SIZE, AO2
	subq	$-8 * SIZE, B
	ALIGN_4

.L76:
	testq	$2, MM
	jle	.L78

	vmovups	0 * SIZE(AO1), %xmm0
	vmovups	1 * SIZE(AO2), %xmm1

	vmovsd	 %xmm0, %xmm5 , %xmm5
	vshufpd	 $1, %xmm1, %xmm0 , %xmm0

	vmovups	%xmm5,  -16 * SIZE(B)
	vmovups	%xmm0,  -14 * SIZE(B)

	addq	$2 * SIZE, AO1
	addq	$2 * SIZE, AO2
	subq	$-4 * SIZE, B
	ALIGN_4

.L78:
	testq	$1, MM
	jle	.L80

	vmovsd	0 * SIZE(AO1),      %xmm0
	vmovsd	0 * SIZE(AO2),      %xmm1

	vunpcklpd %xmm1, %xmm0 , %xmm0

	vmovups	%xmm0,  -16 * SIZE(B)
	subq	$-2 * SIZE, B
	ALIGN_4

.L80:
	testq	$1, N
	jle	.L999

	movq	A, AO1

	testq	$SIZE, A
	jne	.L85

	movq	MM, I
	sarq	$3, I
	jle	.L82
	ALIGN_4

.L81:

	vmovups	0 * SIZE(AO1),      %xmm0
	vmovups	2 * SIZE(AO1),      %xmm2
	vmovups	4 * SIZE(AO1),      %xmm4
	vmovups	6 * SIZE(AO1),      %xmm6


	vmovups	%xmm0,  -16 * SIZE(B)
	vmovups	%xmm2,  -14 * SIZE(B)
	vmovups	%xmm4,  -12 * SIZE(B)
	vmovups	%xmm6,  -10 * SIZE(B)

	addq	$8 * SIZE, AO1
	subq	$-8 * SIZE, B

	decq	I
	jg	.L81
	ALIGN_4

.L82:
	testq	$4, MM
	jle	.L83

	vmovups	0 * SIZE(AO1),      %xmm0
	vmovups	2 * SIZE(AO1),      %xmm2

	vmovups	%xmm0, -16 * SIZE(B)
	vmovups	%xmm2, -14 * SIZE(B)

	addq	$4 * SIZE, AO1
	subq	$-4 * SIZE, B
	ALIGN_4

.L83:
	testq	$2, MM
	jle	.L84

	vmovups	0 * SIZE(AO1),      %xmm0

	vmovups	%xmm0,  -16 * SIZE(B)

	addq	$2 * SIZE, AO1
	subq	$-2 * SIZE, B
	ALIGN_4

.L84:
	testq	$1, MM
	jle	.L999

	vmovsd	0 * SIZE(AO1),      %xmm0

	vmovlpd	%xmm0,  -16 * SIZE(B)
	jmp	.L999
	ALIGN_4

.L85:
	vmovups	-1 * SIZE(AO1),      %xmm0

	movq	M, I
	sarq	$3, I
	jle	.L86
	ALIGN_4

.L86:

	vmovups	1 * SIZE(AO1),      %xmm1
	vmovups	3 * SIZE(AO1),      %xmm2
	vmovups	5 * SIZE(AO1),      %xmm3
	vmovups	7 * SIZE(AO1),      %xmm4

	vshufpd	$1, %xmm1, %xmm0 , %xmm0
	vshufpd	$1, %xmm2, %xmm1 , %xmm1
	vshufpd	$1, %xmm3, %xmm2 , %xmm2
	vshufpd	$1, %xmm4, %xmm3 , %xmm3


	vmovups	%xmm0,  -16 * SIZE(B)
	vmovups	%xmm1,  -14 * SIZE(B)
	vmovups	%xmm2,  -12 * SIZE(B)
	vmovups	%xmm3,  -10 * SIZE(B)

	vmovups	%xmm4, %xmm0

	addq	$8 * SIZE, AO1
	subq	$-8 * SIZE, B

	decq	I
	jg	.L86
	ALIGN_4

.L87:
	testq	$4, M
	jle	.L88

	vmovups	1 * SIZE(AO1),      %xmm1
	vmovups	3 * SIZE(AO1),      %xmm2

	vshufpd	$1, %xmm1, %xmm0 , %xmm0
	vshufpd	$1, %xmm2, %xmm1 , %xmm1

	vmovups	%xmm0,  -16 * SIZE(B)
	vmovups	%xmm1,  -14 * SIZE(B)

	vmovups	%xmm2, %xmm0

	addq	$4 * SIZE, AO1
	addq	$4 * SIZE, B
	ALIGN_4

.L88:
	testq	$2, M
	jle	.L89

	vmovups	1 * SIZE(AO1),      %xmm1

	vshufpd	$1, %xmm1, %xmm0 , %xmm0

	vmovups	%xmm0,  -16 * SIZE(B)

	vmovups	%xmm1, %xmm0

	addq	$2 * SIZE, AO1
	subq	$-2 * SIZE, B
	ALIGN_4

.L89:
	testq	$1, M
	jle	.L999

	vshufpd	$1, %xmm0, %xmm0 , %xmm0

	vmovlpd	%xmm0,  -16 * SIZE(B)
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
