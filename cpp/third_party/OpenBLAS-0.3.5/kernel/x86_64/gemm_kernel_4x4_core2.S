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

#define OLD_M	%rdi
#define OLD_N	%rsi
#define M	%r13
#define N	%r14
#define K	%rdx

#define A	%rcx
#define B	%r8
#define C	%r9
#define LDC	%r10

#define I	%r11
#define AO	%rdi
#define BO	%rsi
#define	CO1	%rbx
#define CO2	%rbp
#define BB	%r12

#ifndef WINDOWS_ABI

#define STACKSIZE 64

#define OLD_LDC		 8 + STACKSIZE(%rsp)
#define OLD_OFFSET	16 + STACKSIZE(%rsp)

#else

#define STACKSIZE 256

#define OLD_A		40 + STACKSIZE(%rsp)
#define OLD_B		48 + STACKSIZE(%rsp)
#define OLD_C		56 + STACKSIZE(%rsp)
#define OLD_LDC		64 + STACKSIZE(%rsp)
#define OLD_OFFSET	72 + STACKSIZE(%rsp)

#endif

#define ALPHA	    0(%rsp)
#define J	   16(%rsp)
#define OFFSET	   24(%rsp)
#define KK	   32(%rsp)
#define KKK	   40(%rsp)
#define BUFFER	  128(%rsp)

#define PREFETCH_R    (8 * 4 + 0)
#define PREFETCH_W    (PREFETCH_R * 4)

#define PREFETCHSIZE  (8 * 13 + 5)
#define PREFETCH     prefetcht0

	PROLOGUE
	PROFCODE

	subq	$STACKSIZE, %rsp
	movq	%rbx,  0(%rsp)
	movq	%rbp,  8(%rsp)
	movq	%r12, 16(%rsp)
	movq	%r13, 24(%rsp)
	movq	%r14, 32(%rsp)
	movq	%r15, 40(%rsp)

#ifdef WINDOWS_ABI
	movq	%rdi,    48(%rsp)
	movq	%rsi,    56(%rsp)
	movups	%xmm6,   64(%rsp)
	movups	%xmm7,   80(%rsp)
	movups	%xmm8,   96(%rsp)
	movups	%xmm9,  112(%rsp)
	movups	%xmm10, 128(%rsp)
	movups	%xmm11, 144(%rsp)
	movups	%xmm12, 160(%rsp)
	movups	%xmm13, 176(%rsp)
	movups	%xmm14, 192(%rsp)
	movups	%xmm15, 208(%rsp)

	movq	ARG1,      OLD_M
	movq	ARG2,      OLD_N
	movq	ARG3,      K
	movq	OLD_A,     A
	movq	OLD_B,     B
	movq	OLD_C,     C
	movq	OLD_LDC,   LDC
#ifdef TRMMKERNEL
	movsd	OLD_OFFSET, %xmm12
#endif
	movaps	%xmm3, %xmm0

#else
	movq	OLD_LDC,   LDC
#ifdef TRMMKERNEL
	movsd	OLD_OFFSET, %xmm12
#endif

#endif

	movq	%rsp, %r15	# save old stack
	subq	$128 + LOCAL_BUFFER_SIZE, %rsp
	andq	$-4096, %rsp	# align stack

	STACK_TOUCHING

	unpcklpd %xmm0, %xmm0
	movapd	 %xmm0, ALPHA

	subq	$-16 * SIZE, A
	subq	$-16 * SIZE, B

	movq	OLD_M, M
	movq	OLD_N, N

	leaq	(, LDC, SIZE), LDC

#ifdef TRMMKERNEL
	movsd	%xmm12, OFFSET
	movsd	%xmm12, KK
#ifndef LEFT
	negq	KK
#endif
#endif

	movq	N,  J
	sarq	$2, J
	NOBRANCH
	jle	.L40
	ALIGN_4

.L01:
/* Copying to Sub Buffer */
	leaq	16 * SIZE + BUFFER, BO

#if defined(TRMMKERNEL) && defined(LEFT)
	movq	OFFSET, %rax
	movq	%rax, KK
#endif

	movapd	 -16 * SIZE(B), %xmm0
	movapd	  -8 * SIZE(B), %xmm4

	movq	K, %rax
	sarq	$2, %rax
	NOBRANCH
	jle	.L05
	ALIGN_3

.L02:
	prefetcht0	(PREFETCH_R + 0) * SIZE(B)
	prefetcht0	(PREFETCH_R + 8) * SIZE(B)

	movapd	 -14 * SIZE(B), %xmm1
	movapd	 -12 * SIZE(B), %xmm2
	movapd	 -10 * SIZE(B), %xmm3
	movapd	  -6 * SIZE(B), %xmm5
	movapd	  -4 * SIZE(B), %xmm6
	movapd	  -2 * SIZE(B), %xmm7

	movddup	  %xmm0, %xmm8
	movapd	%xmm8,  -16 * SIZE(BO)
	unpckhpd  %xmm0, %xmm0
	movapd	%xmm0,  -14 * SIZE(BO)
	movapd	   0 * SIZE(B), %xmm0

	prefetcht0	(PREFETCH_W +  0) * SIZE(BO)
	movddup	  %xmm1, %xmm9
	movapd	%xmm9,  -12 * SIZE(BO)
	unpckhpd  %xmm1, %xmm1
	movapd	%xmm1,  -10 * SIZE(BO)
	movddup	  %xmm2, %xmm10
	movapd	%xmm10,  -8 * SIZE(BO)

	prefetcht0	(PREFETCH_W +  8) * SIZE(BO)
	unpckhpd  %xmm2, %xmm2
	movapd	%xmm2,   -6 * SIZE(BO)
	movddup	  %xmm3, %xmm11
	movapd	%xmm11,  -4 * SIZE(BO)
	unpckhpd  %xmm3, %xmm3
	movapd	%xmm3,   -2 * SIZE(BO)

	prefetcht0	(PREFETCH_W + 16) * SIZE(BO)

	movddup	  %xmm4, %xmm12
	movapd	%xmm12,   0 * SIZE(BO)
	unpckhpd  %xmm4, %xmm4
	movapd	%xmm4,    2 * SIZE(BO)
	movapd	   8 * SIZE(B), %xmm4
	movddup	  %xmm5, %xmm13
	movapd	%xmm13,   4 * SIZE(BO)
	unpckhpd  %xmm5, %xmm5
	movapd	%xmm5,    6 * SIZE(BO)

	prefetcht0	(PREFETCH_W + 24) * SIZE(BO)
	movddup	  %xmm6, %xmm14
	movapd	%xmm14,   8 * SIZE(BO)
	unpckhpd  %xmm6, %xmm6
	movapd	%xmm6,   10 * SIZE(BO)
	movddup	  %xmm7, %xmm15
	movapd	%xmm15,  12 * SIZE(BO)
	unpckhpd  %xmm7, %xmm7
	movapd	%xmm7,   14 * SIZE(BO)

	subq	$-32 * SIZE, BO
	subq	$-16  * SIZE, B
	decq	%rax
	BRANCH
	jne	.L02
	ALIGN_3

.L05:
	movq	K, %rax
	andq	$3, %rax
	BRANCH
	BRANCH
	jle	.L10
	ALIGN_3

.L06:
	movapd	 -14 * SIZE(B), %xmm1

	movddup	  %xmm0, %xmm8
	unpckhpd  %xmm0, %xmm0
	movddup	  %xmm1, %xmm9
	unpckhpd  %xmm1, %xmm1

	movapd	%xmm8,  -16 * SIZE(BO)
	movapd	%xmm0,  -14 * SIZE(BO)
	movapd	 -12 * SIZE(B), %xmm0

	movapd	%xmm9,  -12 * SIZE(BO)
	movapd	%xmm1,  -10 * SIZE(BO)

	addq	$4 * SIZE, B
	addq	$8 * SIZE, BO
	decq	%rax
	BRANCH
	jne	.L06
	ALIGN_4

.L10:
	leaq	(PREFETCH_R +  0) * SIZE(B), BB

	movq	C, CO1			# coffset1 = c
	leaq	(C, LDC, 1), CO2	# coffset2 = c + ldc
	movq	A, AO		# aoffset = a

	movq	M,  I
	sarq	$2, I	# i = (m >> 2)
	NOBRANCH
	jle	.L20
	ALIGN_4

.L11:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	19 * SIZE + BUFFER, BO
#else
	leaq	19 * SIZE + BUFFER, BO
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 8), BO
#endif

	prefetcht2	(BB)

	movaps	-16 * SIZE(AO), %xmm0
	movaps	-14 * SIZE(AO), %xmm1
	movaps	-19 * SIZE(BO), %xmm6
	movaps	-17 * SIZE(BO), %xmm7

	pxor	%xmm2, %xmm2
	prefetcht0     3 * SIZE(CO1)
	pxor	%xmm3, %xmm3
	pxor	%xmm4, %xmm4
	prefetcht0     7 * SIZE(CO2)
	pxor	%xmm5, %xmm5

	movapd	%xmm2, %xmm8
	movapd	%xmm2, %xmm9
	movapd	%xmm2, %xmm10
	prefetcht0     3 * SIZE(CO1, LDC, 2)
	movapd	%xmm2, %xmm11

	movapd	%xmm2, %xmm12
	movapd	%xmm2, %xmm13
	prefetcht0     7 * SIZE(CO2, LDC, 2)
	movapd	%xmm2, %xmm14
	movapd	%xmm2, %xmm15
	subq		$-16 * SIZE, BB

#ifndef TRMMKERNEL
	movq	K, %rax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	movq	K, %rax
	subq	KK, %rax
	movq	%rax, KKK
#else
	movq	KK, %rax
#ifdef LEFT
	addq	$4, %rax
#else
	addq	$4, %rax
#endif
	movq	%rax, KKK
#endif
	sarq	$2, %rax
	NOBRANCH
	jle	.L15
	ALIGN_4

.L12:
	PADDING;
	addpd	%xmm2, %xmm10
	movaps	-15 * SIZE(BO), %xmm2
	PADDING;
	addpd	%xmm3, %xmm14
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AO)
	movaps	%xmm6, %xmm3
	mulpd	%xmm0, %xmm6
	mulpd	%xmm1, %xmm3

	addpd	%xmm4, %xmm11
	movaps	-13 * SIZE(BO), %xmm4
	addpd	%xmm5, %xmm15
	movaps	%xmm7, %xmm5
	mulpd	%xmm0, %xmm7
	mulpd	%xmm1, %xmm5

	addpd	%xmm6, %xmm8
	movaps	-11 * SIZE(BO), %xmm6
	addpd	%xmm3, %xmm12
	movaps	%xmm2, %xmm3
	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm3

	addpd	%xmm7, %xmm9
	movaps	 -9 * SIZE(BO), %xmm7
	addpd	%xmm5, %xmm13
	movaps	 %xmm4, %xmm5
	mulpd	%xmm0, %xmm4
	movaps	-12 * SIZE(AO), %xmm0
	mulpd	%xmm1, %xmm5
	movaps	-10 * SIZE(AO), %xmm1

	addpd	%xmm2, %xmm10
	movaps	 -7 * SIZE(BO), %xmm2
	addpd	%xmm3, %xmm14
	movaps	%xmm6, %xmm3
	mulpd	%xmm0, %xmm6
	mulpd	%xmm1, %xmm3

	addpd	%xmm4, %xmm11
	movaps   -5 * SIZE(BO), %xmm4
	addpd	%xmm5, %xmm15
	movaps	%xmm7, %xmm5
	mulpd	%xmm0, %xmm7
	mulpd	%xmm1, %xmm5

	addpd	%xmm6, %xmm8
	movaps	 -3 * SIZE(BO), %xmm6
	addpd	%xmm3, %xmm12
	movaps	%xmm2, %xmm3
	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm3

	addpd	%xmm7, %xmm9
	movaps	 -1 * SIZE(BO), %xmm7
	addpd	%xmm5, %xmm13
	movaps	 %xmm4, %xmm5
	mulpd	%xmm0, %xmm4
	movaps	 -8 * SIZE(AO), %xmm0
	mulpd	%xmm1, %xmm5
	movaps	 -6 * SIZE(AO), %xmm1

	addpd	%xmm2, %xmm10
	movaps	  1 * SIZE(BO), %xmm2
	addpd	%xmm3, %xmm14
	movaps	%xmm6, %xmm3
	mulpd	%xmm0, %xmm6
	mulpd	%xmm1, %xmm3

	addpd	%xmm4, %xmm11
	movaps	  3 * SIZE(BO), %xmm4
	addpd	%xmm5, %xmm15
	PADDING
	movaps	%xmm7, %xmm5
	mulpd	%xmm1, %xmm5
	PREFETCH (PREFETCHSIZE +  8) * SIZE(AO)
	mulpd	%xmm0, %xmm7

	addpd	%xmm6, %xmm8
	movaps	  5 * SIZE(BO), %xmm6
	addpd	%xmm3, %xmm12
	movaps	%xmm2, %xmm3
	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm3

	addpd	%xmm7, %xmm9
	movaps	  7 * SIZE(BO), %xmm7
	addpd	%xmm5, %xmm13
	movaps	 %xmm4, %xmm5
	mulpd	%xmm0, %xmm4
	movaps	 -4 * SIZE(AO), %xmm0
	mulpd	%xmm1, %xmm5
	movaps	 -2 * SIZE(AO), %xmm1

	addpd	%xmm2, %xmm10
	movaps	  9 * SIZE(BO), %xmm2
	addpd	%xmm3, %xmm14
	movaps	%xmm6, %xmm3
	mulpd	%xmm0, %xmm6
	mulpd	%xmm1, %xmm3

	addpd	%xmm4, %xmm11
	subq	$-16 * SIZE, AO
	movaps	 11 * SIZE(BO), %xmm4
	addpd	%xmm5, %xmm15
	movaps	%xmm7, %xmm5
	mulpd	%xmm0, %xmm7
	mulpd	%xmm1, %xmm5

	addpd	%xmm6, %xmm8
	movaps	 13 * SIZE(BO), %xmm6
	addpd	%xmm3, %xmm12
	movaps	%xmm2, %xmm3
	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm3

	addpd	%xmm7, %xmm9
	movaps	 15 * SIZE(BO), %xmm7
	addpd	%xmm5, %xmm13
	subq	$-32 * SIZE, BO
	movaps	 %xmm4, %xmm5
	mulpd	%xmm0, %xmm4
	movaps	-16 * SIZE(AO), %xmm0
	mulpd	%xmm1, %xmm5
	movaps	-14 * SIZE(AO), %xmm1

	subq	$1, %rax
	BRANCH
	jg	.L12
	ALIGN_3

.L15:
	prefetcht2	-8 * SIZE(BB)

#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	BRANCH
	je	.L18
	ALIGN_3

.L16:
	addpd	%xmm2, %xmm10
	movaps	-15 * SIZE(BO), %xmm2
	addpd	%xmm3, %xmm14
	movaps	%xmm6, %xmm3
	mulpd	%xmm0, %xmm6
	mulpd	%xmm1, %xmm3

	addpd	%xmm4, %xmm11
	movaps	-13 * SIZE(BO), %xmm4
	addpd	%xmm5, %xmm15
	movaps	%xmm7, %xmm5
	mulpd	%xmm0, %xmm7
	mulpd	%xmm1, %xmm5

	addpd	%xmm6, %xmm8
	movaps	-11 * SIZE(BO), %xmm6
	addpd	%xmm3, %xmm12
	addq	$4  * SIZE, AO
	movaps	%xmm2, %xmm3
	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm3

	addpd	%xmm7, %xmm9
	movaps	 -9 * SIZE(BO), %xmm7
	addpd	%xmm5, %xmm13
	addq	$8 * SIZE, BO
	movaps	 %xmm4, %xmm5
	mulpd	%xmm0, %xmm4
	movaps	-16 * SIZE(AO), %xmm0
	mulpd	%xmm1, %xmm5
	movaps	-14 * SIZE(AO), %xmm1

	subq	$1, %rax
	BRANCH
	jg	.L16
	ALIGN_3

.L18:
	movddup	ALPHA, %xmm7

	addpd	%xmm2, %xmm10
	mulpd	%xmm7, %xmm8
	addpd	%xmm3, %xmm14
	mulpd	%xmm7, %xmm12
	addpd	%xmm4, %xmm11
 	mulpd	%xmm7, %xmm9
	addpd	%xmm5, %xmm15
	mulpd	%xmm7, %xmm13

	mulpd	%xmm7, %xmm10
	mulpd	%xmm7, %xmm14
	mulpd	%xmm7, %xmm11
	mulpd	%xmm7, %xmm15

	movq	CO1, %rax
	orq	LDC, %rax
	testq	$15, %rax
	NOBRANCH
	jne	.L18x

#ifndef TRMMKERNEL
	addpd	0 * SIZE(CO1), %xmm8
	addpd	2 * SIZE(CO1), %xmm12
	addpd	0 * SIZE(CO2), %xmm9
	addpd	2 * SIZE(CO2), %xmm13

	addpd	0 * SIZE(CO1, LDC, 2), %xmm10
	addpd	2 * SIZE(CO1, LDC, 2), %xmm14
	addpd	0 * SIZE(CO2, LDC, 2), %xmm11
	addpd	2 * SIZE(CO2, LDC, 2), %xmm15
#endif

	movapd	%xmm8,  0 * SIZE(CO1)
	movapd	%xmm12, 2 * SIZE(CO1)
	movapd	%xmm9,  0 * SIZE(CO2)
	movapd	%xmm13, 2 * SIZE(CO2)

	movapd	%xmm10, 0 * SIZE(CO1, LDC, 2)
	movapd	%xmm14, 2 * SIZE(CO1, LDC, 2)
	movapd	%xmm11, 0 * SIZE(CO2, LDC, 2)
	movapd	%xmm15, 2 * SIZE(CO2, LDC, 2)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 8), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$4, KK
#endif

	addq	$4 * SIZE, CO1		# coffset += 4
	addq	$4 * SIZE, CO2		# coffset += 4
	decq	I			# i --
	BRANCH
	jg	.L11
	jmp	.L20
	ALIGN_4

.L18x:
#ifndef TRMMKERNEL
	movsd	0 * SIZE(CO1), %xmm0
	movhpd	1 * SIZE(CO1), %xmm0
	movsd	2 * SIZE(CO1), %xmm1
	movhpd	3 * SIZE(CO1), %xmm1
	movsd	0 * SIZE(CO2), %xmm2
	movhpd	1 * SIZE(CO2), %xmm2
	movsd	2 * SIZE(CO2), %xmm3
	movhpd	3 * SIZE(CO2), %xmm3

	movsd	0 * SIZE(CO1, LDC, 2), %xmm4
	movhpd	1 * SIZE(CO1, LDC, 2), %xmm4
	movsd	2 * SIZE(CO1, LDC, 2), %xmm5
	movhpd	3 * SIZE(CO1, LDC, 2), %xmm5
	movsd	0 * SIZE(CO2, LDC, 2), %xmm6
	movhpd	1 * SIZE(CO2, LDC, 2), %xmm6
	movsd	2 * SIZE(CO2, LDC, 2), %xmm7
	movhpd	3 * SIZE(CO2, LDC, 2), %xmm7

	addpd	%xmm0, %xmm8
	addpd	%xmm1, %xmm12
	addpd	%xmm2, %xmm9
	addpd	%xmm3, %xmm13
	addpd	%xmm4, %xmm10
	addpd	%xmm5, %xmm14
	addpd	%xmm6, %xmm11
	addpd	%xmm7, %xmm15
#endif

	movsd	%xmm8,  0 * SIZE(CO1)
	movhpd	%xmm8,  1 * SIZE(CO1)
	movsd	%xmm12, 2 * SIZE(CO1)
	movhpd	%xmm12, 3 * SIZE(CO1)
	movsd	%xmm9,  0 * SIZE(CO2)
	movhpd	%xmm9,  1 * SIZE(CO2)
	movsd	%xmm13, 2 * SIZE(CO2)
	movhpd	%xmm13, 3 * SIZE(CO2)

	movsd	%xmm10, 0 * SIZE(CO1, LDC, 2)
	movhpd	%xmm10, 1 * SIZE(CO1, LDC, 2)
	movsd	%xmm14, 2 * SIZE(CO1, LDC, 2)
	movhpd	%xmm14, 3 * SIZE(CO1, LDC, 2)
	movsd	%xmm11, 0 * SIZE(CO2, LDC, 2)
	movhpd	%xmm11, 1 * SIZE(CO2, LDC, 2)
	movsd	%xmm15, 2 * SIZE(CO2, LDC, 2)
	movhpd	%xmm15, 3 * SIZE(CO2, LDC, 2)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 8), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$4, KK
#endif

	addq	$4 * SIZE, CO1		# coffset += 4
	addq	$4 * SIZE, CO2		# coffset += 4
	decq	I			# i --
	BRANCH
	jg	.L11
	ALIGN_4

.L20:
	testq	$2, M
	BRANCH
	jle	.L30

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	16 * SIZE + BUFFER, BO
#else
	leaq	16 * SIZE + BUFFER, BO
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 8), BO
#endif

	pxor	%xmm8, %xmm8
	movapd	-16 * SIZE(AO), %xmm0
	pxor	%xmm9, %xmm9
	pxor	%xmm10, %xmm10
	pxor	%xmm11, %xmm11

	movapd	%xmm8,  %xmm2
	movapd	%xmm9,  %xmm3
	movapd	%xmm10, %xmm4
	movapd	%xmm11, %xmm5

#ifndef TRMMKERNEL
	movq	K, %rax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	movq	K, %rax
	subq	KK, %rax
	movq	%rax, KKK
#else
	movq	KK, %rax
#ifdef LEFT
	addq	$2, %rax
#else
	addq	$4, %rax
#endif
	movq	%rax, KKK
#endif
	sarq	$2, %rax
	NOBRANCH
	jle	.L25
	ALIGN_4

.L21:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AO)
	addpd	%xmm2, %xmm8
	movapd	-16 * SIZE(BO), %xmm2
	mulpd	%xmm0, %xmm2
	addpd	%xmm3, %xmm9
	movapd	-14 * SIZE(BO), %xmm3
	mulpd	%xmm0, %xmm3
	addpd	%xmm4, %xmm10
	movapd	-12 * SIZE(BO), %xmm4
	mulpd	%xmm0, %xmm4
	addpd	%xmm5, %xmm11
	movapd	-10 * SIZE(BO), %xmm5
	mulpd	%xmm0, %xmm5
	movapd	-14 * SIZE(AO), %xmm0

	addpd	%xmm2, %xmm8
	movapd	 -8 * SIZE(BO), %xmm2
	mulpd	%xmm0, %xmm2
	addpd	%xmm3, %xmm9
	movapd	 -6 * SIZE(BO), %xmm3
	mulpd	%xmm0, %xmm3
	addpd	%xmm4, %xmm10
	movapd	 -4 * SIZE(BO), %xmm4
	mulpd	%xmm0, %xmm4
	addpd	%xmm5, %xmm11
	movapd	 -2 * SIZE(BO), %xmm5
	mulpd	%xmm0, %xmm5
	movapd	-12 * SIZE(AO), %xmm0

	addpd	%xmm2, %xmm8
	movapd	  0 * SIZE(BO), %xmm2
	mulpd	%xmm0, %xmm2
	addpd	%xmm3, %xmm9
	movapd	  2 * SIZE(BO), %xmm3
	mulpd	%xmm0, %xmm3
	addpd	%xmm4, %xmm10
	movapd	  4 * SIZE(BO), %xmm4
	mulpd	%xmm0, %xmm4
	addpd	%xmm5, %xmm11
	movapd	  6 * SIZE(BO), %xmm5
	mulpd	%xmm0, %xmm5
	movapd	-10 * SIZE(AO), %xmm0

	addpd	%xmm2, %xmm8
	movapd	  8 * SIZE(BO), %xmm2
	mulpd	%xmm0, %xmm2
	addpd	%xmm3, %xmm9
	movapd	 10 * SIZE(BO), %xmm3
	mulpd	%xmm0, %xmm3
	addpd	%xmm4, %xmm10
	movapd	 12 * SIZE(BO), %xmm4
	mulpd	%xmm0, %xmm4
	addpd	%xmm5, %xmm11
	movapd	 14 * SIZE(BO), %xmm5
	mulpd	%xmm0, %xmm5
	movapd	 -8 * SIZE(AO), %xmm0

	subq	$ -8 * SIZE, AO
	subq	$-32 * SIZE, BO
	subq	$1, %rax
	BRANCH
	jg	.L21
	ALIGN_4

.L25:
	movddup	ALPHA, %xmm7

#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	BRANCH
	je	.L28
	ALIGN_4

.L26:
	addpd	%xmm2, %xmm8
	movapd	-16 * SIZE(BO), %xmm2
	mulpd	%xmm0, %xmm2
	addpd	%xmm3, %xmm9
	movapd	-14 * SIZE(BO), %xmm3
	mulpd	%xmm0, %xmm3
	addpd	%xmm4, %xmm10
	movapd	-12 * SIZE(BO), %xmm4
	mulpd	%xmm0, %xmm4
	addpd	%xmm5, %xmm11
	movapd	-10 * SIZE(BO), %xmm5
	mulpd	%xmm0, %xmm5
	movapd	-14 * SIZE(AO), %xmm0

	addq	$2 * SIZE, AO
	addq	$8 * SIZE, BO
	subq	$1, %rax
	BRANCH
	jg	.L26
	ALIGN_4

.L28:
	addpd	%xmm2, %xmm8
	addpd	%xmm3, %xmm9
	addpd	%xmm4, %xmm10
	addpd	%xmm5, %xmm11

#ifndef TRMMKERNEL
	movsd	0 * SIZE(CO1), %xmm0
	movhpd	1 * SIZE(CO1), %xmm0
	movsd	0 * SIZE(CO2), %xmm2
	movhpd	1 * SIZE(CO2), %xmm2

	movsd	0 * SIZE(CO1, LDC, 2), %xmm4
	movhpd	1 * SIZE(CO1, LDC, 2), %xmm4
	movsd	0 * SIZE(CO2, LDC, 2), %xmm6
	movhpd	1 * SIZE(CO2, LDC, 2), %xmm6
#endif

	mulpd	%xmm7, %xmm8
	mulpd	%xmm7, %xmm9
	mulpd	%xmm7, %xmm10
	mulpd	%xmm7, %xmm11

#ifndef TRMMKERNEL
	addpd	%xmm0,  %xmm8
	addpd	%xmm2, %xmm9
	addpd	%xmm4, %xmm10
	addpd	%xmm6, %xmm11
#endif

	movsd	%xmm8, 0 * SIZE(CO1)
	movhpd	%xmm8, 1 * SIZE(CO1)
	movsd	%xmm9, 0 * SIZE(CO2)
	movhpd	%xmm9, 1 * SIZE(CO2)

	movsd	%xmm10, 0 * SIZE(CO1, LDC, 2)
	movhpd	%xmm10, 1 * SIZE(CO1, LDC, 2)
	movsd	%xmm11, 0 * SIZE(CO2, LDC, 2)
	movhpd	%xmm11, 1 * SIZE(CO2, LDC, 2)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 8), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$2, KK
#endif

	addq	$2 * SIZE, CO1		# coffset += 4
	addq	$2 * SIZE, CO2		# coffset += 4
	ALIGN_4

.L30:
	testq	$1, M
	BRANCH
	jle	.L39

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	16 * SIZE + BUFFER, BO
#else
	leaq	16 * SIZE + BUFFER, BO
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 8), BO
#endif

	pxor	%xmm8, %xmm8
	movsd	-16 * SIZE(AO), %xmm0

	pxor	%xmm9, %xmm9
	pxor	%xmm10, %xmm10
	pxor	%xmm11, %xmm11

	movapd	%xmm8,  %xmm2
	movapd	%xmm9,  %xmm3
	movapd	%xmm10, %xmm4
	movapd	%xmm11, %xmm5

#ifndef TRMMKERNEL
	movq	K, %rax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	movq	K, %rax
	subq	KK, %rax
	movq	%rax, KKK
#else
	movq	KK, %rax
#ifdef LEFT
	addq	$1, %rax
#else
	addq	$4, %rax
#endif
	movq	%rax, KKK
#endif
	sarq	$2, %rax
	NOBRANCH
	jle	.L35
	ALIGN_4

.L31:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AO)
	addsd	%xmm2, %xmm8
	movsd	-16 * SIZE(BO), %xmm2
	mulsd	%xmm0, %xmm2
	addsd	%xmm3, %xmm9
	movsd	-14 * SIZE(BO), %xmm3
	mulsd	%xmm0, %xmm3
	addsd	%xmm4, %xmm10
	movsd	-12 * SIZE(BO), %xmm4
	mulsd	%xmm0, %xmm4
	addsd	%xmm5, %xmm11
	movsd	-10 * SIZE(BO), %xmm5
	mulsd	%xmm0, %xmm5
	movsd	-15 * SIZE(AO), %xmm0

	addsd	%xmm2, %xmm8
	movsd	 -8 * SIZE(BO), %xmm2
	mulsd	%xmm0, %xmm2
	addsd	%xmm3, %xmm9
	movsd	 -6 * SIZE(BO), %xmm3
	mulsd	%xmm0, %xmm3
	addsd	%xmm4, %xmm10
	movsd	 -4 * SIZE(BO), %xmm4
	mulsd	%xmm0, %xmm4
	addsd	%xmm5, %xmm11
	movsd	 -2 * SIZE(BO), %xmm5
	mulsd	%xmm0, %xmm5
	movsd	-14 * SIZE(AO), %xmm0

	addsd	%xmm2, %xmm8
	movsd	  0 * SIZE(BO), %xmm2
	mulsd	%xmm0, %xmm2
	addsd	%xmm3, %xmm9
	movsd	  2 * SIZE(BO), %xmm3
	mulsd	%xmm0, %xmm3
	addsd	%xmm4, %xmm10
	movsd	  4 * SIZE(BO), %xmm4
	mulsd	%xmm0, %xmm4
	addsd	%xmm5, %xmm11
	movsd	  6 * SIZE(BO), %xmm5
	mulsd	%xmm0, %xmm5
	movsd	-13 * SIZE(AO), %xmm0

	addsd	%xmm2, %xmm8
	movsd	  8 * SIZE(BO), %xmm2
	mulsd	%xmm0, %xmm2
	addsd	%xmm3, %xmm9
	movsd	 10 * SIZE(BO), %xmm3
	mulsd	%xmm0, %xmm3
	addsd	%xmm4, %xmm10
	movsd	 12 * SIZE(BO), %xmm4
	mulsd	%xmm0, %xmm4
	addsd	%xmm5, %xmm11
	movsd	 14 * SIZE(BO), %xmm5
	mulsd	%xmm0, %xmm5
	movsd	-12 * SIZE(AO), %xmm0

	subq	$ -4 * SIZE, AO
	subq	$-32 * SIZE, BO
	subq	$1, %rax
	BRANCH
	jg	.L31
	ALIGN_4

.L35:
	movddup	ALPHA, %xmm7

#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	BRANCH
	je	.L38
	ALIGN_4

.L36:
	addsd	%xmm2, %xmm8
	movsd	-16 * SIZE(BO), %xmm2
	mulsd	%xmm0, %xmm2
	addsd	%xmm3, %xmm9
	movsd	-14 * SIZE(BO), %xmm3
	mulsd	%xmm0, %xmm3
	addsd	%xmm4, %xmm10
	movsd	-12 * SIZE(BO), %xmm4
	mulsd	%xmm0, %xmm4
	addsd	%xmm5, %xmm11
	movsd	-10 * SIZE(BO), %xmm5
	mulsd	%xmm0, %xmm5
	movsd	-15 * SIZE(AO), %xmm0

	addq	$1 * SIZE, AO
	addq	$8 * SIZE, BO
	subq	$1, %rax
	BRANCH
	jg	.L36
	ALIGN_4

.L38:
	addsd	%xmm2, %xmm8
	addsd	%xmm3, %xmm9
	addsd	%xmm4, %xmm10
	addsd	%xmm5, %xmm11


#ifndef TRMMKERNEL
	movsd	0 * SIZE(CO1), %xmm0
	movsd	0 * SIZE(CO2), %xmm2
	movsd	0 * SIZE(CO1, LDC, 2), %xmm4
	movsd	0 * SIZE(CO2, LDC, 2), %xmm6
#endif

	mulsd	%xmm7, %xmm8
	mulsd	%xmm7, %xmm9
	mulsd	%xmm7, %xmm10
	mulsd	%xmm7, %xmm11

#ifndef TRMMKERNEL
	addsd	%xmm0,  %xmm8
	addsd	%xmm2, %xmm9
	addsd	%xmm4, %xmm10
	addsd	%xmm6, %xmm11
#endif

	movsd	%xmm8, 0 * SIZE(CO1)
	movsd	%xmm9, 0 * SIZE(CO2)
	movsd	%xmm10, 0 * SIZE(CO1, LDC, 2)
	movsd	%xmm11, 0 * SIZE(CO2, LDC, 2)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 8), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$1, KK
#endif
	ALIGN_4

.L39:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addq	$4, KK
#endif

	leaq	(C, LDC, 4), C
	subq	$1, J
	BRANCH
	jg	.L01
	ALIGN_4

.L40:
	testq	$2, N
	BRANCH
	jle	.L80
	ALIGN_4

.L41:
/* Copying to Sub Buffer */
	leaq	BUFFER, BO

#if defined(TRMMKERNEL) && defined(LEFT)
	movq	OFFSET, %rax
	movq	%rax, KK
#endif

	movq	K, %rax
	sarq	$3, %rax
	jle	.L43

	addq	%rax, %rax
	ALIGN_4

.L42:
	movddup	 -16 * SIZE(B), %xmm8
	movddup	 -15 * SIZE(B), %xmm9
	movddup	 -14 * SIZE(B), %xmm10
	movddup	 -13 * SIZE(B), %xmm11
	movddup	 -12 * SIZE(B), %xmm12
	movddup	 -11 * SIZE(B), %xmm13
	movddup	 -10 * SIZE(B), %xmm14
	movddup	  -9 * SIZE(B), %xmm15

	movapd	%xmm8,   0 * SIZE(BO)
	movapd	%xmm9,   2 * SIZE(BO)
	movapd	%xmm10,  4 * SIZE(BO)
	movapd	%xmm11,  6 * SIZE(BO)
	movapd	%xmm12,  8 * SIZE(BO)
	movapd	%xmm13, 10 * SIZE(BO)
	movapd	%xmm14, 12 * SIZE(BO)
	movapd	%xmm15, 14 * SIZE(BO)

	addq	$8 * SIZE, B
	addq	$16 * SIZE, BO

	subq	$1, %rax
	jne	.L42
	ALIGN_4

.L43:
	movq	K, %rax
	andq	$7, %rax
	BRANCH
	jle	.L45
	ALIGN_4

.L44:
	movddup	 -16 * SIZE(B), %xmm8
	movddup	 -15 * SIZE(B), %xmm9

	movapd	%xmm8,  0 * SIZE(BO)
	movapd	%xmm9,  2 * SIZE(BO)

	addq	$2 * SIZE, B
	addq	$4 * SIZE, BO
	subq	$1, %rax
	jne	.L44
	ALIGN_4

.L45:
	movq	C, CO1
	leaq	(C, LDC, 1), CO2
	movq	A, AO		# aoffset = a

	movq	M,  I
	sarq	$2, I	# i = (m >> 2)
	jle	.L60
	ALIGN_4

.L50:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	16 * SIZE + BUFFER, BO
#else
	leaq	16 * SIZE + BUFFER, BO
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 4), BO
#endif

	pxor	%xmm8, %xmm8
	pxor	%xmm9, %xmm9

	prefetcht0     3 * SIZE(CO1)
	pxor	%xmm12, %xmm12
	prefetcht0     3 * SIZE(CO2)
	pxor	%xmm13, %xmm13

	movapd	-16 * SIZE(AO), %xmm0
	movapd	-14 * SIZE(AO), %xmm1

	movapd	%xmm8, %xmm2
	movapd	%xmm8, %xmm3
	movapd	%xmm8, %xmm4
	movapd	%xmm8, %xmm5


#ifndef TRMMKERNEL
	movq	K, %rax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	movq	K, %rax
	subq	KK, %rax
	movq	%rax, KKK
#else
	movq	KK, %rax
#ifdef LEFT
	addq	$4, %rax
#else
	addq	$2, %rax
#endif
	movq	%rax, KKK
#endif
	sarq	$2, %rax
	jle	.L55
	ALIGN_4

.L51:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AO)

	addpd	%xmm2, %xmm8
	movapd	-16 * SIZE(BO), %xmm2
	addpd	%xmm3, %xmm12
	movapd	 %xmm2, %xmm3
	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm3
	addpd	%xmm4, %xmm9
	movapd	-14 * SIZE(BO), %xmm4
	addpd	%xmm5, %xmm13
	movapd	 %xmm4, %xmm5
	mulpd	%xmm0, %xmm4
	movapd	-12 * SIZE(AO), %xmm0
	mulpd	%xmm1, %xmm5
	movapd	-10 * SIZE(AO), %xmm1

	addpd	%xmm2, %xmm8
	movapd	-12 * SIZE(BO), %xmm2
	addpd	%xmm3, %xmm12
	movapd	 %xmm2, %xmm3
	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm3
	addpd	%xmm4, %xmm9
	movapd	-10 * SIZE(BO), %xmm4
	addpd	%xmm5, %xmm13
	movapd	 %xmm4, %xmm5
	mulpd	%xmm0, %xmm4
	movapd	 -8 * SIZE(AO), %xmm0
	mulpd	%xmm1, %xmm5
	movapd	 -6 * SIZE(AO), %xmm1

	PREFETCH (PREFETCHSIZE +  8) * SIZE(AO)

	addpd	%xmm2, %xmm8
	movapd	 -8 * SIZE(BO), %xmm2
	addpd	%xmm3, %xmm12
	movapd	 %xmm2, %xmm3
	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm3
	addpd	%xmm4, %xmm9
	movapd	 -6 * SIZE(BO), %xmm4
	addpd	%xmm5, %xmm13
	movapd	 %xmm4, %xmm5
	mulpd	%xmm0, %xmm4
	movapd	 -4 * SIZE(AO), %xmm0
	mulpd	%xmm1, %xmm5
	movapd	 -2 * SIZE(AO), %xmm1

	addpd	%xmm2, %xmm8
	movapd	 -4 * SIZE(BO), %xmm2
	addpd	%xmm3, %xmm12
	movapd	 %xmm2, %xmm3
	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm3
	addpd	%xmm4, %xmm9
	movapd	 -2 * SIZE(BO), %xmm4
	addpd	%xmm5, %xmm13
	movapd	 %xmm4, %xmm5
	mulpd	%xmm0, %xmm4
	movapd	  0 * SIZE(AO), %xmm0
	mulpd	%xmm1, %xmm5
	movapd	  2 * SIZE(AO), %xmm1

	subq	$-16 * SIZE, AO
	subq	$-16 * SIZE, BO
	subq	$1, %rax
	jg	.L51
	ALIGN_4

.L55:
	movddup	ALPHA, %xmm7

#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	je .L58
	ALIGN_4

.L56:
	addpd	%xmm2, %xmm8
	movapd	-16 * SIZE(BO), %xmm2
	addpd	%xmm3, %xmm12
	movapd	 %xmm2, %xmm3
	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm3

	addpd	%xmm4, %xmm9
	movapd	-14 * SIZE(BO), %xmm4
	addpd	%xmm5, %xmm13
	movapd	 %xmm4, %xmm5
	mulpd	%xmm0, %xmm4
	movapd	-12 * SIZE(AO), %xmm0
	mulpd	%xmm1, %xmm5
	movapd	-10 * SIZE(AO), %xmm1

	addq	$4 * SIZE, AO
	addq	$4 * SIZE, BO
	subq	$1, %rax
	jg	.L56
	ALIGN_4

.L58:
	addpd	%xmm2, %xmm8
	addpd	%xmm3, %xmm12
	addpd	%xmm4, %xmm9
	addpd	%xmm5, %xmm13


#ifndef TRMMKERNEL
	movsd	0 * SIZE(CO1), %xmm0
	movhpd	1 * SIZE(CO1), %xmm0
	movsd	2 * SIZE(CO1), %xmm1
	movhpd	3 * SIZE(CO1), %xmm1
	movsd	0 * SIZE(CO2), %xmm2
	movhpd	1 * SIZE(CO2), %xmm2
	movsd	2 * SIZE(CO2), %xmm3
	movhpd	3 * SIZE(CO2), %xmm3
#endif

	mulpd	%xmm7, %xmm8
	mulpd	%xmm7, %xmm9
	mulpd	%xmm7, %xmm12
	mulpd	%xmm7, %xmm13

#ifndef TRMMKERNEL
	addpd	%xmm0,  %xmm8
	addpd	%xmm1,  %xmm12
	addpd	%xmm2, %xmm9
	addpd	%xmm3, %xmm13
#endif

	movsd	%xmm8, 0 * SIZE(CO1)
	movhpd	%xmm8, 1 * SIZE(CO1)
	movsd	%xmm12, 2 * SIZE(CO1)
	movhpd	%xmm12, 3 * SIZE(CO1)
	movsd	%xmm9, 0 * SIZE(CO2)
	movhpd	%xmm9, 1 * SIZE(CO2)
	movsd	%xmm13, 2 * SIZE(CO2)
	movhpd	%xmm13, 3 * SIZE(CO2)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 4), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$4, KK
#endif

	addq	$4 * SIZE, CO1
	addq	$4 * SIZE, CO2
	subq	$1, I
	jg	.L50
	ALIGN_4

.L60:
	testq	$2, M
	jle	.L70

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	16 * SIZE + BUFFER, BO
#else
	leaq	16 * SIZE + BUFFER, BO
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 4), BO
#endif

	pxor	%xmm8, %xmm8
	movapd	-16 * SIZE(AO), %xmm0
	pxor	%xmm9, %xmm9
	movapd	-14 * SIZE(AO), %xmm1
	pxor	%xmm10, %xmm10
	pxor	%xmm11, %xmm11

	movapd	%xmm8, %xmm2
	movapd	%xmm8, %xmm3
	movapd	%xmm8, %xmm4
	movapd	%xmm8, %xmm5


#ifndef TRMMKERNEL
	movq	K, %rax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	movq	K, %rax
	subq	KK, %rax
	movq	%rax, KKK
#else
	movq	KK, %rax
#ifdef LEFT
	addq	$2, %rax
#else
	addq	$2, %rax
#endif
	movq	%rax, KKK
#endif
	sarq	$2, %rax
	jle	.L65
	ALIGN_4

.L61:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AO)

	addpd	%xmm2, %xmm8
	movapd	-16 * SIZE(BO), %xmm2
	mulpd	%xmm0, %xmm2
	addpd	%xmm3, %xmm9
	movapd	-14 * SIZE(BO), %xmm3
	mulpd	%xmm0, %xmm3
	movapd	-12 * SIZE(AO), %xmm0
	addpd	%xmm4, %xmm10
	movapd	-12 * SIZE(BO), %xmm4
	mulpd	%xmm1, %xmm4
	addpd	%xmm5, %xmm11
	movapd	-10 * SIZE(BO), %xmm5
	mulpd	%xmm1, %xmm5
	movapd	-10 * SIZE(AO), %xmm1

	addpd	%xmm2, %xmm8
	movapd	 -8 * SIZE(BO), %xmm2
	mulpd	%xmm0, %xmm2
	addpd	%xmm3, %xmm9
	movapd	 -6 * SIZE(BO), %xmm3
	mulpd	%xmm0, %xmm3
	movapd	 -8 * SIZE(AO), %xmm0
	addpd	%xmm4, %xmm10
	movapd	 -4 * SIZE(BO), %xmm4
	mulpd	%xmm1, %xmm4
	addpd	%xmm5, %xmm11
	movapd	 -2 * SIZE(BO), %xmm5
	mulpd	%xmm1, %xmm5
	movapd	 -6 * SIZE(AO), %xmm1

	subq	$ -8 * SIZE, AO
	subq	$-16 * SIZE, BO
	subq	$1, %rax
	jg	.L61
	ALIGN_4

.L65:
	movddup	ALPHA, %xmm7

#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	je .L68
	ALIGN_4

.L66:
	addpd	%xmm2, %xmm8
	movapd	-16 * SIZE(BO), %xmm2
	mulpd	%xmm0, %xmm2
	addpd	%xmm3, %xmm9
	movapd	-14 * SIZE(BO), %xmm3
	mulpd	%xmm0, %xmm3
	movapd	-14 * SIZE(AO), %xmm0

	addq	$2 * SIZE, AO
	addq	$4 * SIZE, BO
	subq	$1, %rax
	jg	.L66
	ALIGN_4

.L68:
	addpd	%xmm2, %xmm8
	addpd	%xmm3, %xmm9
	addpd	%xmm4, %xmm10
	addpd	%xmm5, %xmm11

#ifndef TRMMKERNEL
	movsd	0 * SIZE(CO1), %xmm0
	movhpd	1 * SIZE(CO1), %xmm0
	movsd	0 * SIZE(CO2), %xmm2
	movhpd	1 * SIZE(CO2), %xmm2
#endif

	addpd	%xmm10, %xmm8
	addpd	%xmm11, %xmm9

	mulpd	%xmm7, %xmm8
	mulpd	%xmm7, %xmm9

#ifndef TRMMKERNEL
	addpd	%xmm0,  %xmm8
	addpd	%xmm2, %xmm9
#endif

	movsd	%xmm8, 0 * SIZE(CO1)
	movhpd	%xmm8, 1 * SIZE(CO1)
	movsd	%xmm9, 0 * SIZE(CO2)
	movhpd	%xmm9, 1 * SIZE(CO2)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 4), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$2, KK
#endif

	addq	$2 * SIZE, CO1
	addq	$2 * SIZE, CO2
	ALIGN_4

.L70:
	testq	$1, M
	jle	.L79

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	16 * SIZE + BUFFER, BO
#else
	leaq	16 * SIZE + BUFFER, BO
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 4), BO
#endif

	movsd	-16 * SIZE(AO), %xmm0
	movsd	-15 * SIZE(AO), %xmm1
	pxor	%xmm8, %xmm8
	pxor	%xmm9, %xmm9
	pxor	%xmm10, %xmm10
	pxor	%xmm11, %xmm11

	movapd	%xmm8, %xmm2
	movapd	%xmm8, %xmm3
	movapd	%xmm8, %xmm4
	movapd	%xmm8, %xmm5

#ifndef TRMMKERNEL
	movq	K, %rax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	movq	K, %rax
	subq	KK, %rax
	movq	%rax, KKK
#else
	movq	KK, %rax
#ifdef LEFT
	addq	$1, %rax
#else
	addq	$2, %rax
#endif
	movq	%rax, KKK
#endif
	sarq	$2, %rax
	jle	.L75
	ALIGN_4

.L71:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AO)

	addsd	%xmm2, %xmm8
	movsd	-16 * SIZE(BO), %xmm2
	mulsd	%xmm0, %xmm2
	addsd	%xmm3, %xmm9
	movsd	-14 * SIZE(BO), %xmm3
	mulsd	%xmm0, %xmm3
	movsd	-14 * SIZE(AO), %xmm0
	addsd	%xmm4, %xmm10
	movsd	-12 * SIZE(BO), %xmm4
	mulsd	%xmm1, %xmm4
	addsd	%xmm5, %xmm11
	movsd	-10 * SIZE(BO), %xmm5
	mulsd	%xmm1, %xmm5
	movsd	-13 * SIZE(AO), %xmm1

	addsd	%xmm2, %xmm8
	movsd	 -8 * SIZE(BO), %xmm2
	mulsd	%xmm0, %xmm2
	addsd	%xmm3, %xmm9
	movsd	 -6 * SIZE(BO), %xmm3
	mulsd	%xmm0, %xmm3
	movsd	-12 * SIZE(AO), %xmm0
	addsd	%xmm4, %xmm10
	movsd	 -4 * SIZE(BO), %xmm4
	mulsd	%xmm1, %xmm4
	addsd	%xmm5, %xmm11
	movsd	 -2 * SIZE(BO), %xmm5
	mulsd	%xmm1, %xmm5
	movsd	-11 * SIZE(AO), %xmm1

	subq	$ -4 * SIZE, AO
	subq	$-16 * SIZE, BO
	subq	$1, %rax
	jg	.L71
	ALIGN_4

.L75:
	movddup	ALPHA, %xmm7

#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	je .L78
	ALIGN_4

.L76:
	addsd	%xmm2, %xmm8
	movsd	-16 * SIZE(BO), %xmm2
	mulsd	%xmm0, %xmm2
	addsd	%xmm3, %xmm9
	movsd	-14 * SIZE(BO), %xmm3
	mulsd	%xmm0, %xmm3
	movsd	-15 * SIZE(AO), %xmm0

	addq	$1 * SIZE, AO
	addq	$4 * SIZE, BO
	subq	$1, %rax
	jg	.L76
	ALIGN_4

.L78:
	addsd	%xmm2, %xmm8
	addsd	%xmm3, %xmm9
	addsd	%xmm4, %xmm10
	addsd	%xmm5, %xmm11

#ifndef TRMMKERNEL
	movsd	0 * SIZE(CO1), %xmm0
	movsd	0 * SIZE(CO2), %xmm2
#endif

	addsd	%xmm10, %xmm8
	addsd	%xmm11, %xmm9

	mulsd	%xmm7, %xmm8
	mulsd	%xmm7, %xmm9

#ifndef TRMMKERNEL
	addsd	%xmm0,  %xmm8
	addsd	%xmm2, %xmm9
#endif

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 4), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$1, KK
#endif

	movsd	%xmm8, 0 * SIZE(CO1)
	movsd	%xmm9, 0 * SIZE(CO2)
	ALIGN_4

.L79:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addq	$2, KK
#endif

	leaq	(C, LDC, 2), C
	ALIGN_4

.L80:
	testq	$1, N
	BRANCH
	jle	.L999
	ALIGN_4

.L81:
/* Copying to Sub Buffer */
	leaq	BUFFER, BO

#if defined(TRMMKERNEL) && defined(LEFT)
	movq	OFFSET, %rax
	movq	%rax, KK
#endif

	movq	K, %rax
	sarq	$4, %rax
	jle	.L83

	addq	%rax, %rax
	ALIGN_4

.L82:
	movddup	 -16 * SIZE(B), %xmm8
	movddup	 -15 * SIZE(B), %xmm9
	movddup	 -14 * SIZE(B), %xmm10
	movddup	 -13 * SIZE(B), %xmm11
	movddup	 -12 * SIZE(B), %xmm12
	movddup	 -11 * SIZE(B), %xmm13
	movddup	 -10 * SIZE(B), %xmm14
	movddup	  -9 * SIZE(B), %xmm15

	movapd	%xmm8,  0 * SIZE(BO)
	movapd	%xmm9,  2 * SIZE(BO)
	movapd	%xmm10,  4 * SIZE(BO)
	movapd	%xmm11,  6 * SIZE(BO)
	movapd	%xmm12,  8 * SIZE(BO)
	movapd	%xmm13, 10 * SIZE(BO)
	movapd	%xmm14, 12 * SIZE(BO)
	movapd	%xmm15, 14 * SIZE(BO)

	addq	$  8 * SIZE, B
	subq	$-16 * SIZE, BO
	subq	$1, %rax
	jne	.L82
	ALIGN_4

.L83:
	movq	K, %rax
	andq	$15, %rax
	BRANCH
	jle	.L85
	ALIGN_4

.L84:
	movddup	 -16 * SIZE(B), %xmm8

	movapd	%xmm8,  0 * SIZE(BO)

	addq	$1 * SIZE, B
	addq	$2 * SIZE, BO
	subq	$1, %rax
	jne	.L84
	ALIGN_4

.L85:
	movq	C, CO1
	movq	A, AO

	movq	M,  I
	sarq	$2, I
	jle	.L100
	ALIGN_4

.L90:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	16 * SIZE + BUFFER, BO
#else
	leaq	16 * SIZE + BUFFER, BO
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 2), BO
#endif

	pxor	%xmm8, %xmm8
	movapd	-16 * SIZE(BO), %xmm4
	pxor	%xmm9, %xmm9
	movapd	-14 * SIZE(BO), %xmm5
	pxor	%xmm12, %xmm12
	movapd	-12 * SIZE(BO), %xmm6
	pxor	%xmm13, %xmm13
	movapd	-10 * SIZE(BO), %xmm7

	movapd	%xmm8, %xmm0
	prefetcht0     3 * SIZE(CO1)
	movapd	%xmm8, %xmm1
	movapd	%xmm8, %xmm2
	movapd	%xmm8, %xmm3

#ifndef TRMMKERNEL
	movq	K, %rax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	movq	K, %rax
	subq	KK, %rax
	movq	%rax, KKK
#else
	movq	KK, %rax
#ifdef LEFT
	addq	$4, %rax
#else
	addq	$1, %rax
#endif
	movq	%rax, KKK
#endif
	sarq	$2, %rax
	jle	.L95
	ALIGN_4

.L91:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AO)

	addpd	%xmm0, %xmm8
	movapd	-16 * SIZE(AO), %xmm0
	mulpd	%xmm4, %xmm0
	addpd	%xmm1, %xmm12
	movapd	-14 * SIZE(AO), %xmm1
	mulpd	%xmm4, %xmm1
	movapd	 -8 * SIZE(BO), %xmm4
	addpd	%xmm2, %xmm9
	movapd	-12 * SIZE(AO), %xmm2
	mulpd	%xmm5, %xmm2
	addpd	%xmm3, %xmm13
	movapd	-10 * SIZE(AO), %xmm3
	mulpd	%xmm5, %xmm3
	movapd	 -6 * SIZE(BO), %xmm5

	PREFETCH (PREFETCHSIZE +  8) * SIZE(AO)
	addpd	%xmm0, %xmm8
	movapd	 -8 * SIZE(AO), %xmm0
	mulpd	%xmm6, %xmm0
	addpd	%xmm1, %xmm12
	movapd	 -6 * SIZE(AO), %xmm1
	mulpd	%xmm6, %xmm1
	movapd	 -4 * SIZE(BO), %xmm6
	addpd	%xmm2, %xmm9
	movapd	 -4 * SIZE(AO), %xmm2
	mulpd	%xmm7, %xmm2
	addpd	%xmm3, %xmm13
	movapd	 -2 * SIZE(AO), %xmm3
	mulpd	%xmm7, %xmm3
	movapd	 -2 * SIZE(BO), %xmm7

	subq	$-16 * SIZE, AO
	subq	$ -8 * SIZE, BO
	subq	$1, %rax
	jg	.L91
	ALIGN_4

.L95:
	movddup	ALPHA, %xmm7

#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	je .L98
	ALIGN_4

.L96:
	addpd	%xmm0, %xmm8
	movapd	-16 * SIZE(AO), %xmm0
	mulpd	%xmm4, %xmm0
	addpd	%xmm1, %xmm12
	movapd	-14 * SIZE(AO), %xmm1
	mulpd	%xmm4, %xmm1
	movapd	-14 * SIZE(BO), %xmm4

	addq	$4 * SIZE, AO
	addq	$2 * SIZE, BO
	subq	$1, %rax
	jg	.L96
	ALIGN_4

.L98:
	addpd	%xmm0, %xmm8
	addpd	%xmm1, %xmm12
	addpd	%xmm2, %xmm9
	addpd	%xmm3, %xmm13

	addpd	%xmm9, %xmm8
	addpd	%xmm13, %xmm12

#ifndef TRMMKERNEL
	movsd	0 * SIZE(CO1), %xmm0
	movhpd	1 * SIZE(CO1), %xmm0
	movsd	2 * SIZE(CO1), %xmm1
	movhpd	3 * SIZE(CO1), %xmm1
#endif

	mulpd	%xmm7, %xmm8
	mulpd	%xmm7, %xmm12

#ifndef TRMMKERNEL
	addpd	%xmm0,  %xmm8
	addpd	%xmm1,  %xmm12
#endif

	movsd	%xmm8, 0 * SIZE(CO1)
	movhpd	%xmm8, 1 * SIZE(CO1)
	movsd	%xmm12, 2 * SIZE(CO1)
	movhpd	%xmm12, 3 * SIZE(CO1)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 2), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$4, KK
#endif

	addq	$4 * SIZE, CO1
	subq	$1, I
	jg	.L90
	ALIGN_4

.L100:
	testq	$2, M
	jle	.L110

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	16 * SIZE + BUFFER, BO
#else
	leaq	16 * SIZE + BUFFER, BO
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 2), BO
#endif

	pxor	%xmm8, %xmm8
	movapd	-16 * SIZE(BO), %xmm4
	pxor	%xmm9, %xmm9
	movapd	-14 * SIZE(BO), %xmm5
	pxor	%xmm10, %xmm10
	movapd	-12 * SIZE(BO), %xmm6
	pxor	%xmm11, %xmm11
	movapd	-10 * SIZE(BO), %xmm7

	movapd	%xmm8, %xmm0
	movapd	%xmm8, %xmm1
	movapd	%xmm8, %xmm2
	movapd	%xmm8, %xmm3

#ifndef TRMMKERNEL
	movq	K, %rax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	movq	K, %rax
	subq	KK, %rax
	movq	%rax, KKK
#else
	movq	KK, %rax
#ifdef LEFT
	addq	$2, %rax
#else
	addq	$1, %rax
#endif
	movq	%rax, KKK
#endif
	sarq	$2, %rax
	jle	.L105
	ALIGN_4

.L101:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AO)

	addpd	%xmm0, %xmm8
	movapd	-16 * SIZE(AO), %xmm0
	mulpd	%xmm4, %xmm0
	movapd	 -8 * SIZE(BO), %xmm4
	addpd	%xmm1, %xmm9
	movapd	-14 * SIZE(AO), %xmm1
	mulpd	%xmm5, %xmm1
	movapd	 -6 * SIZE(BO), %xmm5
	addpd	%xmm2, %xmm10
	movapd	-12 * SIZE(AO), %xmm2
	mulpd	%xmm6, %xmm2
	movapd	 -4 * SIZE(BO), %xmm6
	addpd	%xmm3, %xmm11
	movapd	-10 * SIZE(AO), %xmm3
	mulpd	%xmm7, %xmm3
	movapd	 -2 * SIZE(BO), %xmm7

	subq	$-8 * SIZE, AO
	subq	$-8 * SIZE, BO
	subq	$1, %rax
	jg	.L101
	ALIGN_4

.L105:
	movddup	ALPHA, %xmm7

#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	je .L108
	ALIGN_4

.L106:
	addpd	%xmm0, %xmm8
	movapd	-16 * SIZE(AO), %xmm0
	mulpd	%xmm4, %xmm0
	movapd	-14 * SIZE(BO), %xmm4

	addq	$2 * SIZE, AO
	addq	$2 * SIZE, BO
	subq	$1, %rax
	jg	.L106
	ALIGN_4

.L108:
	addpd	%xmm0, %xmm8
	addpd	%xmm1, %xmm9
	addpd	%xmm2, %xmm10
	addpd	%xmm3, %xmm11

	addpd	%xmm10, %xmm8
	addpd	%xmm11, %xmm9

	addpd	%xmm9, %xmm8

#ifndef TRMMKERNEL
	movsd	0 * SIZE(CO1), %xmm0
	movhpd	1 * SIZE(CO1), %xmm0
#endif

	mulpd	%xmm7, %xmm8
#ifndef TRMMKERNEL
	addpd	%xmm0,  %xmm8
#endif

	movsd	%xmm8, 0 * SIZE(CO1)
	movhpd	%xmm8, 1 * SIZE(CO1)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 2), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$2, KK
#endif
	addq	$2 * SIZE, CO1
	addq	$2 * SIZE, CO2
	ALIGN_4

.L110:
	testq	$1, M
	jle	.L999

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	16 * SIZE + BUFFER, BO
#else
	leaq	16 * SIZE + BUFFER, BO
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 2), BO
#endif

	pxor	%xmm8, %xmm8
	movsd	-16 * SIZE(BO), %xmm4
	pxor	%xmm9, %xmm9
	movsd	-14 * SIZE(BO), %xmm5
	pxor	%xmm10, %xmm10
	movsd	-12 * SIZE(BO), %xmm6
	pxor	%xmm11, %xmm11
	movsd	-10 * SIZE(BO), %xmm7

	movapd	%xmm8, %xmm0
	movapd	%xmm8, %xmm1
	movapd	%xmm8, %xmm2
	movapd	%xmm8, %xmm3

#ifndef TRMMKERNEL
	movq	K, %rax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	movq	K, %rax
	subq	KK, %rax
	movq	%rax, KKK
#else
	movq	KK, %rax
#ifdef LEFT
	addq	$1, %rax
#else
	addq	$1, %rax
#endif
	movq	%rax, KKK
#endif
	sarq	$2, %rax
	jle	.L115
	ALIGN_4

.L111:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AO)

	addpd	%xmm0, %xmm8
	movsd	-16 * SIZE(AO), %xmm0
	mulpd	%xmm4, %xmm0
	movsd	 -8 * SIZE(BO), %xmm4
	addpd	%xmm1, %xmm9
	movsd	-15 * SIZE(AO), %xmm1
	mulpd	%xmm5, %xmm1
	movsd	 -6 * SIZE(BO), %xmm5
	addpd	%xmm2, %xmm10
	movsd	-14 * SIZE(AO), %xmm2
	mulpd	%xmm6, %xmm2
	movsd	 -4 * SIZE(BO), %xmm6
	addpd	%xmm3, %xmm11
	movsd	-13 * SIZE(AO), %xmm3
	mulpd	%xmm7, %xmm3
	movsd	 -2 * SIZE(BO), %xmm7

	subq	$-4 * SIZE, AO
	subq	$-8 * SIZE, BO
	subq	$1, %rax
	jg	.L111
	ALIGN_4

.L115:
	movddup	ALPHA, %xmm7

#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	je .L118
	ALIGN_4

.L116:
	addsd	%xmm0, %xmm8
	movsd	-16 * SIZE(AO), %xmm0
	mulsd	%xmm4, %xmm0
	movsd	-14 * SIZE(BO), %xmm4

	addq	$1 * SIZE, AO
	addq	$2 * SIZE, BO
	subq	$1, %rax
	jg	.L116
	ALIGN_4

.L118:
	addsd	%xmm0, %xmm8
	addsd	%xmm1, %xmm9
	addsd	%xmm2, %xmm10
	addsd	%xmm3, %xmm11

#ifndef TRMMKERNEL
	movsd	0 * SIZE(CO1), %xmm0
#endif
	addsd	%xmm10, %xmm8
	addsd	%xmm11, %xmm9

	addsd	%xmm9, %xmm8

	mulsd	%xmm7, %xmm8
#ifndef TRMMKERNEL
	addsd	%xmm0,  %xmm8
#endif
	movsd	%xmm8, 0 * SIZE(CO1)
	ALIGN_4

.L999:
	movq	%r15, %rsp

	movq	  0(%rsp), %rbx
	movq	  8(%rsp), %rbp
	movq	 16(%rsp), %r12
	movq	 24(%rsp), %r13
	movq	 32(%rsp), %r14
	movq	 40(%rsp), %r15

#ifdef WINDOWS_ABI
	movq	 48(%rsp), %rdi
	movq	 56(%rsp), %rsi
	movups	 64(%rsp), %xmm6
	movups	 80(%rsp), %xmm7
	movups	 96(%rsp), %xmm8
	movups	112(%rsp), %xmm9
	movups	128(%rsp), %xmm10
	movups	144(%rsp), %xmm11
	movups	160(%rsp), %xmm12
	movups	176(%rsp), %xmm13
	movups	192(%rsp), %xmm14
	movups	208(%rsp), %xmm15
#endif

	addq	$STACKSIZE, %rsp
	ret

	EPILOGUE
