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

#define OLD_ALPHA_I	40 + STACKSIZE(%rsp)
#define OLD_A		48 + STACKSIZE(%rsp)
#define OLD_B		56 + STACKSIZE(%rsp)
#define OLD_C		64 + STACKSIZE(%rsp)
#define OLD_LDC		72 + STACKSIZE(%rsp)
#define OLD_OFFSET	80 + STACKSIZE(%rsp)

#endif

#define ALPHA_R	  0(%rsp)
#define ALPHA_I	 16(%rsp)
#define J	 32(%rsp)
#define OFFSET	 40(%rsp)
#define KK	 48(%rsp)
#define KKK	 56(%rsp)
#define BUFFER	128(%rsp)

#define PREFETCH_R    (8 * 4 + 0)
#define PREFETCH_W    (PREFETCH_R * 2)

#define PREFETCHSIZE  (8 * 13 + 5)
#define PREFETCH     prefetcht0

#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
#define ADD1	  addpd
#define ADD2	  addpd
#elif defined(NR) || defined(NC) || defined(TR) || defined(TC)
#define ADD1	  addpd
#define ADD2	  subpd
#elif defined(RN) || defined(RT) || defined(CN) || defined(CT)
#define ADD1	  subpd
#define ADD2	  addpd
#else
#define ADD1	  subpd
#define ADD2	  subpd
#endif

#define ADDSUB	subpd

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
	movsd	OLD_ALPHA_I, %xmm1
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

	movddup	%xmm0, %xmm0
	movddup	%xmm1, %xmm1

	movapd	 %xmm0, ALPHA_R
	movapd	 %xmm1, ALPHA_I

	subq	$-16 * SIZE, A
	subq	$-16 * SIZE, B

	movq	OLD_M, M
	movq	OLD_N, N

#ifdef TRMMKERNEL
	movsd	%xmm12, OFFSET
	movsd	%xmm12, KK
#ifndef LEFT
	negq	KK
#endif
#endif

	salq	$ZBASE_SHIFT, LDC

	movq	N,  J
	sarq	$1, J		# j = (n >> 2)
	NOBRANCH
	jle	.L100
	ALIGN_4

.L01:
#if defined(TRMMKERNEL) && defined(LEFT)
	movq	OFFSET, %rax
	movq	%rax, KK
#endif

/* Copying to Sub Buffer */
	leaq	16 * SIZE +  BUFFER, BO

	movapd	 -16 * SIZE(B), %xmm0
	movapd	  -8 * SIZE(B), %xmm4

	movq	K, %rax
	sarq	$2, %rax
	jle	.L03
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
	jne	.L02
	ALIGN_3

.L03:
	movq	K, %rax
	andq	$3, %rax
	BRANCH
	jle	.L05
	ALIGN_3

.L04:
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

	addq	$ 4 * SIZE, B
	addq	$ 8 * SIZE, BO

	decq	%rax
	jne	.L04
	ALIGN_3

.L05:
	leaq	(PREFETCH_R +  0) * SIZE(B), BB

	movq	C, CO1			# coffset1 = c
	leaq	(C, LDC, 1), CO2	# coffset2 = c + ldc
	movq	A, AO		# aoffset = a

	movq	M,  I
	sarq	$1, I		# i = (m >> 2)
	jle	.L30
	ALIGN_4

.L10:
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

	movaps	-16 * SIZE(AO), %xmm0
	movaps	-14 * SIZE(AO), %xmm1
	movaps	-19 * SIZE(BO), %xmm6
	movaps	-17 * SIZE(BO), %xmm7

	prefetcht2	0 * SIZE(BB)

	pxor	%xmm8, %xmm8
	pxor	%xmm9, %xmm9
	prefetcht2	8 * SIZE(BB)
	pxor	%xmm10, %xmm10
	pxor	%xmm11, %xmm11

	pxor	%xmm12, %xmm12
	prefetcht0     3 * SIZE(CO1)
	pxor	%xmm13, %xmm13
	pxor	%xmm14, %xmm14
	pxor	%xmm15, %xmm15

	pxor	%xmm2, %xmm2
	pxor	%xmm3, %xmm3
	prefetcht0     3 * SIZE(CO2)
	pxor	%xmm4, %xmm4
	pxor	%xmm5, %xmm5

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
	addq	$2, %rax
#else
	addq	$2, %rax
#endif
	movq	%rax, KKK
#endif
	sarq	$2, %rax
	NOBRANCH
	jle	.L15
	ALIGN_4

.L12:
	PADDING;
	ADD1	%xmm2, %xmm10
	movaps	-15 * SIZE(BO), %xmm2
	PADDING;
	ADD1	%xmm3, %xmm14
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AO)
	movaps	%xmm6, %xmm3
	mulpd	%xmm0, %xmm6
	mulpd	%xmm1, %xmm3

	ADD2	%xmm4, %xmm11
	movaps	-13 * SIZE(BO), %xmm4
	ADD2	%xmm5, %xmm15
	movaps	%xmm7, %xmm5
	mulpd	%xmm0, %xmm7
	mulpd	%xmm1, %xmm5

	ADD1	%xmm6, %xmm8
	movaps	-11 * SIZE(BO), %xmm6
	ADD1	%xmm3, %xmm12
	movaps	%xmm2, %xmm3
	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm3

	ADD2	%xmm7, %xmm9
	movaps	 -9 * SIZE(BO), %xmm7
	ADD2	%xmm5, %xmm13
	movaps	 %xmm4, %xmm5
	mulpd	%xmm0, %xmm4
	movaps	-12 * SIZE(AO), %xmm0
	mulpd	%xmm1, %xmm5
	movaps	-10 * SIZE(AO), %xmm1

	ADD1	%xmm2, %xmm10
	movaps	 -7 * SIZE(BO), %xmm2
	ADD1	%xmm3, %xmm14
	movaps	%xmm6, %xmm3
	mulpd	%xmm0, %xmm6
	mulpd	%xmm1, %xmm3

	ADD2	%xmm4, %xmm11
	movaps   -5 * SIZE(BO), %xmm4
	ADD2	%xmm5, %xmm15
	movaps	%xmm7, %xmm5
	mulpd	%xmm0, %xmm7
	mulpd	%xmm1, %xmm5

	ADD1	%xmm6, %xmm8
	movaps	 -3 * SIZE(BO), %xmm6
	ADD1	%xmm3, %xmm12
	movaps	%xmm2, %xmm3
	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm3

	ADD2	%xmm7, %xmm9
	movaps	 -1 * SIZE(BO), %xmm7
	ADD2	%xmm5, %xmm13
	movaps	 %xmm4, %xmm5
	mulpd	%xmm0, %xmm4
	movaps	 -8 * SIZE(AO), %xmm0
	mulpd	%xmm1, %xmm5
	movaps	 -6 * SIZE(AO), %xmm1

	ADD1	%xmm2, %xmm10
	movaps	  1 * SIZE(BO), %xmm2
	ADD1	%xmm3, %xmm14
	movaps	%xmm6, %xmm3
	mulpd	%xmm0, %xmm6
	mulpd	%xmm1, %xmm3

	ADD2	%xmm4, %xmm11
	movaps	  3 * SIZE(BO), %xmm4
	ADD2	%xmm5, %xmm15
	PADDING
	movaps	%xmm7, %xmm5
	mulpd	%xmm1, %xmm5
	PREFETCH (PREFETCHSIZE +  8) * SIZE(AO)
	mulpd	%xmm0, %xmm7

	ADD1	%xmm6, %xmm8
	movaps	  5 * SIZE(BO), %xmm6
	ADD1	%xmm3, %xmm12
	movaps	%xmm2, %xmm3
	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm3

	ADD2	%xmm7, %xmm9
	movaps	  7 * SIZE(BO), %xmm7
	ADD2	%xmm5, %xmm13
	movaps	 %xmm4, %xmm5
	mulpd	%xmm0, %xmm4
	movaps	 -4 * SIZE(AO), %xmm0
	mulpd	%xmm1, %xmm5
	movaps	 -2 * SIZE(AO), %xmm1

	ADD1	%xmm2, %xmm10
	movaps	  9 * SIZE(BO), %xmm2
	ADD1	%xmm3, %xmm14
	movaps	%xmm6, %xmm3
	mulpd	%xmm0, %xmm6
	mulpd	%xmm1, %xmm3

	ADD2	%xmm4, %xmm11
	subq	$-16 * SIZE, AO
	movaps	 11 * SIZE(BO), %xmm4
	ADD2	%xmm5, %xmm15
	movaps	%xmm7, %xmm5
	mulpd	%xmm0, %xmm7
	mulpd	%xmm1, %xmm5

	ADD1	%xmm6, %xmm8
	movaps	 13 * SIZE(BO), %xmm6
	ADD1	%xmm3, %xmm12
	movaps	%xmm2, %xmm3
	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm3

	ADD2	%xmm7, %xmm9
	movaps	 15 * SIZE(BO), %xmm7
	ADD2	%xmm5, %xmm13
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
	andq	$3, %rax
	BRANCH
	BRANCH
	je	.L19
	ALIGN_4

.L16:
	ADD1	%xmm2, %xmm10
	movaps	-15 * SIZE(BO), %xmm2
	ADD1	%xmm3, %xmm14
	movaps	%xmm6, %xmm3
	mulpd	%xmm0, %xmm6
	mulpd	%xmm1, %xmm3

	ADD2	%xmm4, %xmm11
	movaps	-13 * SIZE(BO), %xmm4
	ADD2	%xmm5, %xmm15
	movaps	%xmm7, %xmm5
	mulpd	%xmm0, %xmm7
	mulpd	%xmm1, %xmm5

	ADD1	%xmm6, %xmm8
	movaps	-11 * SIZE(BO), %xmm6
	ADD1	%xmm3, %xmm12
	addq	$4  * SIZE, AO
	movaps	%xmm2, %xmm3
	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm3

	ADD2	%xmm7, %xmm9
	movaps	 -9 * SIZE(BO), %xmm7
	ADD2	%xmm5, %xmm13
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

.L19:
	movapd	ALPHA_R, %xmm6
	ADD1	%xmm2, %xmm10
	ADD1	%xmm3, %xmm14
	movapd	ALPHA_I, %xmm7
	ADD2	%xmm4, %xmm11
	ADD2	%xmm5, %xmm15

	SHUFPD_1 %xmm9, %xmm9
	SHUFPD_1 %xmm11, %xmm11
	SHUFPD_1 %xmm13, %xmm13
	SHUFPD_1 %xmm15, %xmm15

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(NR) || defined(NC) || defined(TR) || defined(TC)

	addsubpd	%xmm9, %xmm8
	addsubpd	%xmm11, %xmm10
	addsubpd	%xmm13, %xmm12
	addsubpd	%xmm15, %xmm14

	movapd	%xmm8, %xmm9
	movapd	%xmm10, %xmm11
	movapd	%xmm12, %xmm13
	movapd	%xmm14, %xmm15
#else
	addsubpd	%xmm8, %xmm9
	addsubpd	%xmm10, %xmm11
	addsubpd	%xmm12, %xmm13
	addsubpd	%xmm14, %xmm15

	movapd		%xmm9, %xmm8
	movapd		%xmm11, %xmm10
	movapd		%xmm13, %xmm12
	movapd		%xmm15, %xmm14
#endif

#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	movsd	0 * SIZE(CO1), %xmm0
	movhpd	1 * SIZE(CO1), %xmm0
	movsd	2 * SIZE(CO1), %xmm2
	movhpd	3 * SIZE(CO1), %xmm2

	movsd	0 * SIZE(CO2), %xmm1
	movhpd	1 * SIZE(CO2), %xmm1
	movsd	2 * SIZE(CO2), %xmm3
	movhpd	3 * SIZE(CO2), %xmm3
#endif

	SHUFPD_1 %xmm9, %xmm9
	SHUFPD_1 %xmm11, %xmm11
	SHUFPD_1 %xmm13, %xmm13
	SHUFPD_1 %xmm15, %xmm15

	mulpd	%xmm6, %xmm8
	mulpd	%xmm6, %xmm10
	mulpd	%xmm6, %xmm12
	mulpd	%xmm6, %xmm14

	mulpd	%xmm7, %xmm9
	mulpd	%xmm7, %xmm11
	mulpd	%xmm7, %xmm13
	mulpd	%xmm7, %xmm15

	addsubpd	%xmm9, %xmm8
	addsubpd	%xmm11, %xmm10
	addsubpd	%xmm13, %xmm12
	addsubpd	%xmm15, %xmm14

#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	addpd	%xmm0,  %xmm8
	addpd	%xmm1,  %xmm10
	addpd	%xmm2, %xmm12
	addpd	%xmm3, %xmm14
#endif

	movsd	%xmm8, 0 * SIZE(CO1)
	movhpd	%xmm8, 1 * SIZE(CO1)
	movsd	%xmm12, 2 * SIZE(CO1)
	movhpd	%xmm12, 3 * SIZE(CO1)

	movsd	%xmm10, 0 * SIZE(CO2)
	movhpd	%xmm10, 1 * SIZE(CO2)
	movsd	%xmm14, 2 * SIZE(CO2)
	movhpd	%xmm14, 3 * SIZE(CO2)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 4), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$2, KK
#endif

	addq	$4 * SIZE, CO1		# coffset += 4
	addq	$4 * SIZE, CO2		# coffset += 4
	decq	I			# i --
	jg	.L10
	ALIGN_4

.L30:
	testq	$1, M
	jle	.L99

.L40:
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
	pxor	%xmm9, %xmm9
	pxor	%xmm10, %xmm10
	pxor	%xmm11, %xmm11

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
	je	.L42

.L41:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AO)

	movapd	-16 * SIZE(AO), %xmm0
	movapd	-16 * SIZE(BO), %xmm2
	movapd	-14 * SIZE(BO), %xmm3
	movapd	-12 * SIZE(BO), %xmm4
	movapd	-10 * SIZE(BO), %xmm5

	mulpd	%xmm0, %xmm2
	mulpd	%xmm0, %xmm3
	mulpd	%xmm0, %xmm4
	mulpd	%xmm0, %xmm5

	ADD1	%xmm2, %xmm8
	ADD2	%xmm3, %xmm9
	ADD1	%xmm4, %xmm10
	ADD2	%xmm5, %xmm11

	movapd	-14 * SIZE(AO), %xmm0
	movapd	 -8 * SIZE(BO), %xmm2
	movapd	 -6 * SIZE(BO), %xmm3
	movapd	 -4 * SIZE(BO), %xmm4
	movapd	 -2 * SIZE(BO), %xmm5

	mulpd	%xmm0, %xmm2
	mulpd	%xmm0, %xmm3
	mulpd	%xmm0, %xmm4
	mulpd	%xmm0, %xmm5

	ADD1	%xmm2, %xmm8
	ADD2	%xmm3, %xmm9
	ADD1	%xmm4, %xmm10
	ADD2	%xmm5, %xmm11

	movapd	-12 * SIZE(AO), %xmm0
	movapd	  0 * SIZE(BO), %xmm2
	movapd	  2 * SIZE(BO), %xmm3
	movapd	  4 * SIZE(BO), %xmm4
	movapd	  6 * SIZE(BO), %xmm5

	mulpd	%xmm0, %xmm2
	mulpd	%xmm0, %xmm3
	mulpd	%xmm0, %xmm4
	mulpd	%xmm0, %xmm5

	ADD1	%xmm2, %xmm8
	ADD2	%xmm3, %xmm9
	ADD1	%xmm4, %xmm10
	ADD2	%xmm5, %xmm11

	movapd	-10 * SIZE(AO), %xmm0
	movapd	  8 * SIZE(BO), %xmm2
	movapd	 10 * SIZE(BO), %xmm3
	movapd	 12 * SIZE(BO), %xmm4
	movapd	 14 * SIZE(BO), %xmm5

	mulpd	%xmm0, %xmm2
	mulpd	%xmm0, %xmm3
	mulpd	%xmm0, %xmm4
	mulpd	%xmm0, %xmm5

	ADD1	%xmm2, %xmm8
	ADD2	%xmm3, %xmm9
	ADD1	%xmm4, %xmm10
	ADD2	%xmm5, %xmm11

	subq	$ -8 * SIZE, AO
	subq	$-32 * SIZE, BO
	subq	$1, %rax
	jne    .L41

.L42:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	BRANCH
	jle .L44

.L43:
	movapd	-16 * SIZE(AO), %xmm0
	movapd	-16 * SIZE(BO), %xmm2
	movapd	-14 * SIZE(BO), %xmm3
	movapd	-12 * SIZE(BO), %xmm4
	movapd	-10 * SIZE(BO), %xmm5

	mulpd	%xmm0, %xmm2
	mulpd	%xmm0, %xmm3
	mulpd	%xmm0, %xmm4
	mulpd	%xmm0, %xmm5

	ADD1	%xmm2, %xmm8
	ADD2	%xmm3, %xmm9
	ADD1	%xmm4, %xmm10
	ADD2	%xmm5, %xmm11

	addq	$2 * SIZE, AO
	addq	$8 * SIZE, BO
	subq	$1, %rax
	jg	.L43
	ALIGN_4

.L44:
	movapd	ALPHA_R, %xmm6
	movapd	ALPHA_I, %xmm7

	SHUFPD_1 %xmm9, %xmm9
	SHUFPD_1 %xmm11, %xmm11

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(NR) || defined(NC) || defined(TR) || defined(TC)

	addsubpd	%xmm9, %xmm8
	addsubpd	%xmm11, %xmm10

	movapd	%xmm8, %xmm9
	movapd	%xmm10, %xmm11
#else
	addsubpd	%xmm8, %xmm9
	addsubpd	%xmm10, %xmm11

	movapd		%xmm9, %xmm8
	movapd		%xmm11, %xmm10
#endif

#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	movsd	0 * SIZE(CO1), %xmm0
	movhpd	1 * SIZE(CO1), %xmm0

	movsd	0 * SIZE(CO2), %xmm1
	movhpd	1 * SIZE(CO2), %xmm1
#endif

	SHUFPD_1 %xmm9, %xmm9
	SHUFPD_1 %xmm11, %xmm11

	mulpd	%xmm6, %xmm8
	mulpd	%xmm6, %xmm10

	mulpd	%xmm7, %xmm9
	mulpd	%xmm7, %xmm11

	addsubpd	%xmm9, %xmm8
	addsubpd	%xmm11, %xmm10

#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	addpd	%xmm0,  %xmm8
	addpd	%xmm1,  %xmm10
#endif

	movsd	%xmm8, 0 * SIZE(CO1)
	movhpd	%xmm8, 1 * SIZE(CO1)
	movsd	%xmm10, 0 * SIZE(CO2)
	movhpd	%xmm10, 1 * SIZE(CO2)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 8), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$1, KK
#endif
	ALIGN_4

.L99:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addl	$2, KK
#endif

	leaq	(C, LDC, 2), C		# c += 2 * ldc
	decq	J			# j --
	jg	.L01

.L100:
	testq	$1, N
	jle	.L999

.L101:
#if defined(TRMMKERNEL) && defined(LEFT)
	movq	OFFSET, %rax
	movq	%rax, KK
#endif

/* Copying to Sub Buffer */
	leaq	BUFFER, BO

	movq	K, %rax
	sarq	$2, %rax
	jle	.L103
	ALIGN_4

.L102:
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
	decq	%rax
	jne	.L102
	ALIGN_4

.L103:
	movq	K, %rax
	andq	$3, %rax
	BRANCH
	jle	.L105
	ALIGN_4

.L104:
	movddup	 -16 * SIZE(B), %xmm8
	movddup	 -15 * SIZE(B), %xmm9

	movapd	%xmm8,  0 * SIZE(BO)
	movapd	%xmm9,  2 * SIZE(BO)

	addq	$4 * SIZE, BO
	addq	$2 * SIZE, B
	decq	%rax
	jne	.L104
	ALIGN_4

.L105:
	movq	C, CO1		# coffset1 = c
	movq	A, AO		# aoffset = a

	movq	M,  I
	sarq	$1, I		# i = (m >> 2)
	jle	.L130
	ALIGN_4

.L110:
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
	pxor	%xmm12, %xmm12
	pxor	%xmm13, %xmm13
	prefetcht0     3 * SIZE(CO1)

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
	je	.L112

.L111:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AO)

	movapd	-16 * SIZE(AO), %xmm0
	movapd	-14 * SIZE(AO), %xmm1

	movapd	-16 * SIZE(BO), %xmm2
	movapd	 %xmm2, %xmm3
	movapd	-14 * SIZE(BO), %xmm4
	movapd	 %xmm4, %xmm5

	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm3
	mulpd	%xmm0, %xmm4
	mulpd	%xmm1, %xmm5

	ADD1	%xmm2, %xmm8
	ADD1	%xmm3, %xmm12
	ADD2	%xmm4, %xmm9
	ADD2	%xmm5, %xmm13

	movapd	-12 * SIZE(AO), %xmm0
	movapd	-10 * SIZE(AO), %xmm1

	movapd	-12 * SIZE(BO), %xmm2
	movapd	 %xmm2, %xmm3
	movapd	-10 * SIZE(BO), %xmm4
	movapd	 %xmm4, %xmm5

	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm3
	mulpd	%xmm0, %xmm4
	mulpd	%xmm1, %xmm5

	ADD1	%xmm2, %xmm8
	ADD1	%xmm3, %xmm12
	ADD2	%xmm4, %xmm9
	ADD2	%xmm5, %xmm13

	movapd	 -8 * SIZE(AO), %xmm0
	movapd	 -6 * SIZE(AO), %xmm1

	movapd	 -8 * SIZE(BO), %xmm2
	movapd	 %xmm2, %xmm3
	movapd	 -6 * SIZE(BO), %xmm4
	movapd	 %xmm4, %xmm5

	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm3
	mulpd	%xmm0, %xmm4
	mulpd	%xmm1, %xmm5

	ADD1	%xmm2, %xmm8
	ADD1	%xmm3, %xmm12
	ADD2	%xmm4, %xmm9
	ADD2	%xmm5, %xmm13

	movapd	 -4 * SIZE(AO), %xmm0
	movapd	 -2 * SIZE(AO), %xmm1

	movapd	 -4 * SIZE(BO), %xmm2
	movapd	 %xmm2, %xmm3
	movapd	 -2 * SIZE(BO), %xmm4
	movapd	 %xmm4, %xmm5

	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm3
	mulpd	%xmm0, %xmm4
	mulpd	%xmm1, %xmm5

	ADD1	%xmm2, %xmm8
	ADD1	%xmm3, %xmm12
	ADD2	%xmm4, %xmm9
	ADD2	%xmm5, %xmm13

	subq	$-16 * SIZE, AO
	subq	$-16 * SIZE, BO
	subq	$1, %rax
	jne    .L111
	ALIGN_4

.L112:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	BRANCH
	jle .L114

.L113:
	movapd	-16 * SIZE(AO), %xmm0
	movapd	-14 * SIZE(AO), %xmm1

	movapd	-16 * SIZE(BO), %xmm2
	movapd	 %xmm2, %xmm3
	movapd	-14 * SIZE(BO), %xmm4
	movapd	 %xmm4, %xmm5

	mulpd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm3
	mulpd	%xmm0, %xmm4
	mulpd	%xmm1, %xmm5

	ADD1	%xmm2, %xmm8
	ADD1	%xmm3, %xmm12
	ADD2	%xmm4, %xmm9
	ADD2	%xmm5, %xmm13

	addq	$4 * SIZE, AO
	addq	$4 * SIZE, BO
	subq	$1, %rax
	jg	.L113
	ALIGN_4

.L114:
	movapd	ALPHA_R, %xmm6
	movapd	ALPHA_I, %xmm7

	SHUFPD_1 %xmm9, %xmm9
	SHUFPD_1 %xmm13, %xmm13

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(NR) || defined(NC) || defined(TR) || defined(TC)

	addsubpd	%xmm9, %xmm8
	addsubpd	%xmm13, %xmm12

	movapd	%xmm8, %xmm9
	movapd	%xmm12, %xmm13
#else
	addsubpd	%xmm8, %xmm9
	addsubpd	%xmm12, %xmm13

	movapd		%xmm9, %xmm8
	movapd		%xmm13, %xmm12
#endif

#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	movsd	0 * SIZE(CO1), %xmm0
	movhpd	1 * SIZE(CO1), %xmm0
	movsd	2 * SIZE(CO1), %xmm2
	movhpd	3 * SIZE(CO1), %xmm2
#endif

	SHUFPD_1 %xmm9, %xmm9
	SHUFPD_1 %xmm13, %xmm13

	mulpd	%xmm6, %xmm8
	mulpd	%xmm6, %xmm12

	mulpd	%xmm7, %xmm9
	mulpd	%xmm7, %xmm13

	addsubpd	%xmm9, %xmm8
	addsubpd	%xmm13, %xmm12

#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	addpd	%xmm0,  %xmm8
	addpd	%xmm2, %xmm12
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
	leaq	(BO, %rax, 4), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$2, KK
#endif

	addq	$4 * SIZE, CO1		# coffset += 4
	decq	I			# i --
	jg	.L110
	ALIGN_4

.L130:
	testq	$1, M
	jle	.L999
	ALIGN_4

.L140:
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
	pxor	%xmm9, %xmm9
	pxor	%xmm10, %xmm10
	pxor	%xmm11, %xmm11

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
	je	.L142

.L141:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AO)

	movapd	-16 * SIZE(AO), %xmm0
	movapd	-14 * SIZE(AO), %xmm1
	movapd	-16 * SIZE(BO), %xmm2
	movapd	-14 * SIZE(BO), %xmm3
	movapd	-12 * SIZE(BO), %xmm4
	movapd	-10 * SIZE(BO), %xmm5

	mulpd	%xmm0, %xmm2
	mulpd	%xmm0, %xmm3
	mulpd	%xmm1, %xmm4
	mulpd	%xmm1, %xmm5

	ADD1	%xmm2, %xmm8
	ADD2	%xmm3, %xmm9
	ADD1	%xmm4, %xmm10
	ADD2	%xmm5, %xmm11

	movapd	-12 * SIZE(AO), %xmm0
	movapd	-10 * SIZE(AO), %xmm1
	movapd	 -8 * SIZE(BO), %xmm2
	movapd	 -6 * SIZE(BO), %xmm3
	movapd	 -4 * SIZE(BO), %xmm4
	movapd	 -2 * SIZE(BO), %xmm5

	mulpd	%xmm0, %xmm2
	mulpd	%xmm0, %xmm3
	mulpd	%xmm1, %xmm4
	mulpd	%xmm1, %xmm5

	ADD1	%xmm2, %xmm8
	ADD2	%xmm3, %xmm9
	ADD1	%xmm4, %xmm10
	ADD2	%xmm5, %xmm11

	subq	$ -8 * SIZE, AO
	subq	$-16 * SIZE, BO
	subq	$1, %rax
	jne    .L141

.L142:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	BRANCH
	jle .L144

.L143:
	movapd	-16 * SIZE(AO), %xmm0
	movapd	-16 * SIZE(BO), %xmm2
	movapd	-14 * SIZE(BO), %xmm3

	mulpd	%xmm0, %xmm2
	mulpd	%xmm0, %xmm3

	ADD1	%xmm2, %xmm8
	ADD2	%xmm3, %xmm9

	addq	$2 * SIZE, AO
	addq	$4 * SIZE, BO
	subq	$1, %rax
	jg	.L143
	ALIGN_4

.L144:
	movapd	ALPHA_R, %xmm6
	movapd	ALPHA_I, %xmm7

	addpd	%xmm10, %xmm8
	addpd	%xmm11, %xmm9

	SHUFPD_1 %xmm9, %xmm9

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(NR) || defined(NC) || defined(TR) || defined(TC)

	addsubpd	%xmm9, %xmm8
	movapd	%xmm8, %xmm9
#else
	addsubpd	%xmm8, %xmm9
	movapd		%xmm9, %xmm8
#endif

#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	movsd	0 * SIZE(CO1), %xmm0
	movhpd	1 * SIZE(CO1), %xmm0
#endif

	SHUFPD_1 %xmm9, %xmm9
	mulpd	%xmm6, %xmm8
	mulpd	%xmm7, %xmm9
	addsubpd	%xmm9, %xmm8

#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	addpd	%xmm0,  %xmm8
#endif

	movsd	%xmm8, 0 * SIZE(CO1)
	movhpd	%xmm8, 1 * SIZE(CO1)
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
