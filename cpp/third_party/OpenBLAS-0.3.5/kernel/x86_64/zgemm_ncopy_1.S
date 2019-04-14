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
#define J	%r10
#define AO1	%r11
#define AO2	%r12
#else

#define STACKSIZE 256

#define M	ARG1	/* rcx */
#define N	ARG2	/* rdx */
#define A	ARG3	/* r8  */
#define LDA	ARG4	/* r9  */
#define OLD_B		40 + 24 + STACKSIZE(%rsp)

#define B	%r10
#define I	%r11
#define J	%r12
#define AO1	%r13
#define AO2	%r14

#endif

#define RPREFETCHSIZE 32
#define WPREFETCHSIZE 48

	PROLOGUE
	PROFCODE

#ifdef WINDOWS_ABI
	pushq	%r14
	pushq	%r13
#endif
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

	salq	$ZBASE_SHIFT, LDA

	testq	N, N
	movq	N, J
	jle	.L999
	ALIGN_4

.L12:
	movq	A, AO1
	addq	LDA, A

	movq	M,  I
	sarq	$2, I
	jle	.L14
	ALIGN_4

.L13:
#ifndef DOUBLE
	movsd	0 * SIZE(AO1), %xmm0
	movhps	2 * SIZE(AO1), %xmm0
	movsd	4 * SIZE(AO1), %xmm1
	movhps	6 * SIZE(AO1), %xmm1

	movaps	%xmm0,   0 * SIZE(B)
	movaps	%xmm1,   4 * SIZE(B)
#else
	movsd	0 * SIZE(AO1), %xmm0
	movhpd	1 * SIZE(AO1), %xmm0
	movsd	2 * SIZE(AO1), %xmm1
	movhpd	3 * SIZE(AO1), %xmm1
	movsd	4 * SIZE(AO1), %xmm2
	movhpd	5 * SIZE(AO1), %xmm2
	movsd	6 * SIZE(AO1), %xmm3
	movhpd	7 * SIZE(AO1), %xmm3

	prefetcht2	RPREFETCHSIZE * SIZE(AO1)

	movapd	%xmm0,   0 * SIZE(B)
	movapd	%xmm1,   2 * SIZE(B)
	movapd	%xmm2,   4 * SIZE(B)
	movapd	%xmm3,   6 * SIZE(B)

	prefetcht2	WPREFETCHSIZE * SIZE(B)
#endif

	addq	$8 * SIZE, AO1
	addq	$8 * SIZE, B
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
	movsd	0 * SIZE(AO1), %xmm0
	movsd	%xmm0,   0 * SIZE(B)
#else
	movsd	0 * SIZE(AO1), %xmm0
	movhpd	1 * SIZE(AO1), %xmm0

	movapd	%xmm0,   0 * SIZE(B)
#endif

	addq	$2 * SIZE, AO1
	addq	$2 * SIZE, B
	decq	I
	jg	.L15
	ALIGN_4

.L16:
	decq	J
	jg	.L12
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
#ifdef WINDOWS_ABI
	popq	%r13
	popq	%r14
#endif
	ret

	EPILOGUE
