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

#define STACK	16
#define ARGS	 0

#define STACK_M	 4 + STACK + ARGS(%esi)
#define STACK_N	 8 + STACK + ARGS(%esi)
#define STACK_K	12 + STACK + ARGS(%esi)
#define STACK_ALPHA_R	16 + STACK + ARGS(%esi)
#define STACK_ALPHA_I	24 + STACK + ARGS(%esi)
#define STACK_A	32 + STACK + ARGS(%esi)
#define STACK_B	36 + STACK + ARGS(%esi)
#define STACK_C	40 + STACK + ARGS(%esi)
#define STACK_LDC	44 + STACK + ARGS(%esi)
#define STACK_OFFT	48 + STACK + ARGS(%esi)

#define POSINV	 0(%esp)
#define K	16(%esp)
#define N	20(%esp)
#define M	24(%esp)
#define A	28(%esp)
#define C	32(%esp)
#define J	36(%esp)
#define OLD_STACK 40(%esp)
#define OFFSET  44(%esp)
#define KK	48(%esp)
#define KKK	52(%esp)
#define AORIG   56(%esp)
#define BORIG	60(%esp)
#define BUFFER 128(%esp)

#define STACK_ALIGN	4096
#define STACK_OFFSET	1024

#if defined(OPTERON) || defined(BARCELONA)
#define PREFETCH	prefetch
#else
#define PREFETCH	prefetcht0
#endif

#define PREFETCHSIZE (8 * 10 + 4)

#define AA	%edx
#define BB	%ecx
#define LDC	%ebp
#define B	%edi
#define CO1	%esi

#ifndef CONJ
#define NN
#else
#if defined(LN) || defined(LT)
#define CN
#else
#define NC
#endif
#endif

#define KERNEL1(address) \
	mulpd	%xmm0, %xmm2; \
	addpd	%xmm2, %xmm4; \
	PREFETCH (PREFETCHSIZE +  0) * SIZE + (address) * 1 * SIZE(AA); \
	movapd	 2 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	mulpd	%xmm0, %xmm2; \
	addpd	%xmm2, %xmm5; \
	movapd	 4 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	mulpd	%xmm0, %xmm2; \
	mulpd	 6 * SIZE + (address) * 4 * SIZE(BB), %xmm0; \
	addpd	%xmm2, %xmm6; \
	movapd	16 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	addpd	%xmm0, %xmm7; \
	movapd	 2 * SIZE + (address) * 1 * SIZE(AA), %xmm0

#define KERNEL2(address) \
	mulpd	%xmm0, %xmm3; \
	addpd	%xmm3, %xmm4; \
	movapd	10 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	mulpd	%xmm0, %xmm3; \
	addpd	%xmm3, %xmm5; \
	movapd	12 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	mulpd	%xmm0, %xmm3; \
	mulpd	14 * SIZE + (address) * 4 * SIZE(BB), %xmm0; \
	addpd	%xmm3, %xmm6; \
	movapd	24 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	addpd	%xmm0, %xmm7; \
	movapd	 4 * SIZE + (address) * 1 * SIZE(AA), %xmm0

#define KERNEL3(address) \
	mulpd	%xmm0, %xmm2; \
	addpd	%xmm2, %xmm4; \
	movapd	18 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	mulpd	%xmm0, %xmm2; \
	addpd	%xmm2, %xmm5; \
	movapd	20 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	mulpd	%xmm0, %xmm2; \
	mulpd	22 * SIZE + (address) * 4 * SIZE(BB), %xmm0; \
	addpd	%xmm2, %xmm6; \
	movapd	32 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	addpd	%xmm0, %xmm7; \
	movapd	 6 * SIZE + (address) * 1 * SIZE(AA), %xmm0

#define KERNEL4(address) \
	mulpd	%xmm0, %xmm3; \
	addpd	%xmm3, %xmm4; \
	movapd	26 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	mulpd	%xmm0, %xmm3; \
	addpd	%xmm3, %xmm5; \
	movapd	28 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	mulpd	%xmm0, %xmm3; \
	mulpd	30 * SIZE + (address) * 4 * SIZE(BB), %xmm0; \
	addpd	%xmm3, %xmm6; \
	movapd	40 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	addpd	%xmm0, %xmm7; \
	movapd	16 * SIZE + (address) * 1 * SIZE(AA), %xmm0

#define KERNEL5(address) \
	PREFETCH (PREFETCHSIZE + 8) * SIZE + (address) * 1 * SIZE(AA); \
	mulpd	%xmm1, %xmm2; \
	addpd	%xmm2, %xmm4; \
	movapd	34 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	mulpd	%xmm1, %xmm2; \
	addpd	%xmm2, %xmm5; \
	movapd	36 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	mulpd	%xmm1, %xmm2; \
	mulpd	38 * SIZE + (address) * 4 * SIZE(BB), %xmm1; \
	addpd	%xmm2, %xmm6; \
	movapd	48 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	addpd	%xmm1, %xmm7; \
	movapd	10 * SIZE + (address) * 1 * SIZE(AA), %xmm1

#define KERNEL6(address) \
	mulpd	%xmm1, %xmm3; \
	addpd	%xmm3, %xmm4; \
	movapd	42 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	mulpd	%xmm1, %xmm3; \
	addpd	%xmm3, %xmm5; \
	movapd	44 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	mulpd	%xmm1, %xmm3; \
	mulpd	46 * SIZE + (address) * 4 * SIZE(BB), %xmm1; \
	addpd	%xmm3, %xmm6; \
	movapd	56 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	addpd	%xmm1, %xmm7; \
	movapd	12 * SIZE + (address) * 1 * SIZE(AA), %xmm1

#define KERNEL7(address) \
	mulpd	%xmm1, %xmm2; \
	addpd	%xmm2, %xmm4; \
	movapd	50 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	mulpd	%xmm1, %xmm2; \
	addpd	%xmm2, %xmm5; \
	movapd	52 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	mulpd	%xmm1, %xmm2; \
	mulpd	54 * SIZE + (address) * 4 * SIZE(BB), %xmm1; \
	addpd	%xmm2, %xmm6; \
	movapd	64 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	addpd	%xmm1, %xmm7; \
	movapd	14 * SIZE + (address) * 1 * SIZE(AA), %xmm1

#define KERNEL8(address) \
	mulpd	%xmm1, %xmm3; \
	addpd	%xmm3, %xmm4; \
	movapd	58 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	mulpd	%xmm1, %xmm3; \
	addpd	%xmm3, %xmm5; \
	movapd	60 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	mulpd	%xmm1, %xmm3; \
	mulpd	62 * SIZE + (address) * 4 * SIZE(BB), %xmm1; \
	addpd	%xmm3, %xmm6; \
	movapd	72 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	addpd	%xmm1, %xmm7; \
	movapd	24 * SIZE + (address) * 1 * SIZE(AA), %xmm1

	PROLOGUE

	pushl	%ebp
	pushl	%edi
	pushl	%esi
	pushl	%ebx

	PROFCODE

	EMMS

	movl	%esp, %esi	# save old stack

	subl	$128 + LOCAL_BUFFER_SIZE + STACK_OFFSET, %esp
	andl	$-STACK_ALIGN, %esp	# align stack
	addl	$STACK_OFFSET, %esp

	STACK_TOUCHING

	movl	STACK_M, %ebx
	movl	STACK_N, %eax
	movl	STACK_K, %ecx
	movl	STACK_A, %edx

	movl	%ebx, M
	movl	%eax, N
	movl	%ecx, K
	movl	%edx, A
	movl	%esi, OLD_STACK

	movl	STACK_B, B
	movl	STACK_C, %ebx
	movss	STACK_OFFT, %xmm4

	pcmpeqb	%xmm7, %xmm7
	psllq	$63, %xmm7	# Generate mask
	pxor	%xmm2, %xmm2

	movlpd	  %xmm2,  0 + POSINV
	movlpd	  %xmm7,  8 + POSINV

	movl	%ebx, C
	movl	STACK_LDC, LDC

	movss	%xmm4, OFFSET
	movss	%xmm4, KK

	sall	$ZBASE_SHIFT, LDC

#ifdef LN
       movl	M, %eax
       sall	$ZBASE_SHIFT, %eax
       addl	%eax, C
       imull	K, %eax
       addl	%eax, A
#endif

#ifdef RT
       movl	N, %eax
       sall	$ZBASE_SHIFT, %eax
       imull	K, %eax
       addl	%eax, B

       movl	N, %eax
       imull	LDC, %eax
       addl	%eax, C
#endif

#ifdef RN
	negl	KK
#endif

#ifdef RT
       movl	N, %eax
       subl	OFFSET, %eax
       movl	%eax, KK
#endif

	movl	N, %eax
	sarl	$1, %eax
	movl	%eax, J			# j = n
	jle	.L100
	ALIGN_4

.L01:
#ifdef LN
	movl	OFFSET, %eax
	addl	M, %eax
	movl	%eax, KK
#endif

	leal	BUFFER, BB

#ifdef RT
       movl	K, %eax
       sall	$1 + ZBASE_SHIFT, %eax
       subl	%eax, B
#endif

#if defined(LN) || defined(RT)
	movl	KK, %eax
	movl	B, BORIG
	sall	$1 + ZBASE_SHIFT, %eax
	addl	%eax, B
	leal	(BB, %eax, 2), BB
#endif

#if defined(LT)
	movl	OFFSET, %eax
	movl	%eax, KK
#endif

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K,  %eax
	subl	KK, %eax
#endif
	sarl	$1, %eax
	jle	.L03
	ALIGN_4

.L02:
	prefetchnta	 56 * SIZE(B)

	movlpd	 0 * SIZE(B), %xmm0
	movlpd	 1 * SIZE(B), %xmm1
	movlpd	 2 * SIZE(B), %xmm2
	movlpd	 3 * SIZE(B), %xmm3
	movlpd	 4 * SIZE(B), %xmm4
	movlpd	 5 * SIZE(B), %xmm5
	movlpd	 6 * SIZE(B), %xmm6
	movlpd	 7 * SIZE(B), %xmm7

	movlpd	%xmm0,  0 * SIZE(BB)
	movlpd	%xmm0,  1 * SIZE(BB)
	movlpd	%xmm1,  2 * SIZE(BB)
	movlpd	%xmm1,  3 * SIZE(BB)
	movlpd	%xmm2,  4 * SIZE(BB)
	movlpd	%xmm2,  5 * SIZE(BB)
	movlpd	%xmm3,  6 * SIZE(BB)
	movlpd	%xmm3,  7 * SIZE(BB)
	movlpd	%xmm4,  8 * SIZE(BB)
	movlpd	%xmm4,  9 * SIZE(BB)
	movlpd	%xmm5, 10 * SIZE(BB)
	movlpd	%xmm5, 11 * SIZE(BB)
	movlpd	%xmm6, 12 * SIZE(BB)
	movlpd	%xmm6, 13 * SIZE(BB)
	movlpd	%xmm7, 14 * SIZE(BB)
	movlpd	%xmm7, 15 * SIZE(BB)

	addl	$  8 * SIZE, B
	subl	$-16 * SIZE, BB

	decl	%eax
	jne	.L02
	ALIGN_4

.L03:
#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	andl	$1, %eax
	BRANCH
	jle	.L05

	movlpd	 0 * SIZE(B), %xmm0
	movlpd	 1 * SIZE(B), %xmm1
	movlpd	 2 * SIZE(B), %xmm2
	movlpd	 3 * SIZE(B), %xmm3

	movlpd	%xmm0,  0 * SIZE(BB)
	movlpd	%xmm0,  1 * SIZE(BB)
	movlpd	%xmm1,  2 * SIZE(BB)
	movlpd	%xmm1,  3 * SIZE(BB)
	movlpd	%xmm2,  4 * SIZE(BB)
	movlpd	%xmm2,  5 * SIZE(BB)
	movlpd	%xmm3,  6 * SIZE(BB)
	movlpd	%xmm3,  7 * SIZE(BB)

	addl	$4 * SIZE, B
	ALIGN_4

.L05:
#if defined(LT) || defined(RN)
	movl	A, %eax
	movl	%eax, AA
#else
	movl	A, %eax
	movl	%eax, AORIG
#endif

#ifdef RT
       leal	(, LDC, 2), %eax
       subl	%eax, C
#endif

	movl	C, CO1

#ifndef RT
	leal	(, LDC, 2), %eax
	addl	%eax, C
#endif

	movl	M,  %ebx
	testl	%ebx, %ebx
	jle	.L100
	ALIGN_4

.L10:
#ifdef LN
       movl	K, %eax
       sall	$ZBASE_SHIFT, %eax
       subl	%eax, AORIG
#endif

#if defined(LN) || defined(RT)
	movl	AORIG, %eax
	movl	%eax, AA

	movl	KK, %eax
	sall	$ZBASE_SHIFT, %eax
	addl	%eax, AA
#endif

	leal	BUFFER, BB

#if defined(LN) || defined(RT)
	movl	KK, %eax
	sall	$1 + ZBASE_SHIFT, %eax
	leal	(BB, %eax, 2), BB
#endif

	movapd	 0 * SIZE(AA), %xmm0
	pxor	%xmm4, %xmm4
	movapd	 8 * SIZE(AA), %xmm1
	pxor	%xmm5, %xmm5
	movapd	 0 * SIZE(BB), %xmm2
	pxor	%xmm6, %xmm6
	movapd	 8 * SIZE(BB), %xmm3
	pxor	%xmm7, %xmm7

#ifdef LN
	prefetchw -2 * SIZE(CO1)
	prefetchw -2 * SIZE(CO1, LDC)
#else
	prefetchw  2 * SIZE(CO1)
	prefetchw  2 * SIZE(CO1, LDC)
#endif

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif

#if 1
	andl	$-8, %eax
	sall	$4, %eax
	je	.L15
.L1X:
	KERNEL1(16  *  0)
	KERNEL2(16  *  0)
	KERNEL3(16  *  0)
	KERNEL4(16  *  0)
	KERNEL5(16  *  0)
	KERNEL6(16  *  0)
	KERNEL7(16  *  0)
	KERNEL8(16  *  0)
	cmpl	$128 *  1, %eax
	jle	.L12
	KERNEL1(16  *  1)
	KERNEL2(16  *  1)
	KERNEL3(16  *  1)
	KERNEL4(16  *  1)
	KERNEL5(16  *  1)
	KERNEL6(16  *  1)
	KERNEL7(16  *  1)
	KERNEL8(16  *  1)
	cmpl	$128 *  2, %eax
	jle	.L12
	KERNEL1(16  *  2)
	KERNEL2(16  *  2)
	KERNEL3(16  *  2)
	KERNEL4(16  *  2)
	KERNEL5(16  *  2)
	KERNEL6(16  *  2)
	KERNEL7(16  *  2)
	KERNEL8(16  *  2)
	cmpl	$128 *  3, %eax
	jle	.L12
	KERNEL1(16  *  3)
	KERNEL2(16  *  3)
	KERNEL3(16  *  3)
	KERNEL4(16  *  3)
	KERNEL5(16  *  3)
	KERNEL6(16  *  3)
	KERNEL7(16  *  3)
	KERNEL8(16  *  3)
	cmpl	$128 *  4, %eax
	jle	.L12
	KERNEL1(16  *  4)
	KERNEL2(16  *  4)
	KERNEL3(16  *  4)
	KERNEL4(16  *  4)
	KERNEL5(16  *  4)
	KERNEL6(16  *  4)
	KERNEL7(16  *  4)
	KERNEL8(16  *  4)
	cmpl	$128 *  5, %eax
	jle	.L12
	KERNEL1(16  *  5)
	KERNEL2(16  *  5)
	KERNEL3(16  *  5)
	KERNEL4(16  *  5)
	KERNEL5(16  *  5)
	KERNEL6(16  *  5)
	KERNEL7(16  *  5)
	KERNEL8(16  *  5)
	cmpl	$128 *  6, %eax
	jle	.L12
	KERNEL1(16  *  6)
	KERNEL2(16  *  6)
	KERNEL3(16  *  6)
	KERNEL4(16  *  6)
	KERNEL5(16  *  6)
	KERNEL6(16  *  6)
	KERNEL7(16  *  6)
	KERNEL8(16  *  6)
	cmpl	$128 *  7, %eax
	jle	.L12
	KERNEL1(16  *  7)
	KERNEL2(16  *  7)
	KERNEL3(16  *  7)
	KERNEL4(16  *  7)
	KERNEL5(16  *  7)
	KERNEL6(16  *  7)
	KERNEL7(16  *  7)
	KERNEL8(16  *  7)

	addl	$128 * 4  * SIZE, BB
	addl	$128 * 1  * SIZE, AA
	subl	$128 * 8, %eax
	jg	.L1X
	jmp	.L15

.L12:
	leal	(AA, %eax, 1), AA
	leal	(BB, %eax, 4), BB
	ALIGN_4
#else

	sarl	$3, %eax
	je	.L15
	ALIGN_4

.L12:
	KERNEL1(16  *  0)
	KERNEL2(16  *  0)
	KERNEL3(16  *  0)
	KERNEL4(16  *  0)
	KERNEL5(16  *  0)
	KERNEL6(16  *  0)
	KERNEL7(16  *  0)
	KERNEL8(16  *  0)

	addl   $64 * SIZE, BB
	addl   $16 * SIZE, AA
	decl   %eax
	jne    .L11
	ALIGN_4
#endif

.L15:
#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K,  %eax
	subl	KK, %eax
#endif
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L14
	ALIGN_4

.L13:
	mulpd	 %xmm0, %xmm2
	addpd	 %xmm2, %xmm4
	movapd	 2 * SIZE(BB), %xmm2
	mulpd	 %xmm0, %xmm2
	addpd	 %xmm2, %xmm5
	movapd	 4 * SIZE(BB), %xmm2
	mulpd	 %xmm0, %xmm2
	mulpd	 6 * SIZE(BB), %xmm0
	addpd	 %xmm2, %xmm6
	movapd	 8 * SIZE(BB), %xmm2
	addpd	 %xmm0, %xmm7
	movapd	 2 * SIZE(AA), %xmm0

	addl	$2 * SIZE, AA
	addl	$8 * SIZE, BB
	decl	%eax
	jg	.L13
	ALIGN_4

.L14:
#if defined(LN) || defined(RT)
	movl	KK, %eax
#ifdef LN
	subl	$1, %eax
#else
	subl	$2, %eax
#endif

	movl	AORIG, AA
	movl	BORIG, B
	leal	BUFFER, BB

	sall	$ZBASE_SHIFT, %eax
	addl	%eax, AA
	leal	(B,  %eax, 2), B
	leal	(BB, %eax, 4), BB
#endif

	movapd	POSINV,  %xmm1

	SHUFPD_1 %xmm5, %xmm5
	SHUFPD_1 %xmm7, %xmm7

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(NR) || defined(NC) || defined(TR) || defined(TC)
	xorpd	%xmm1, %xmm5
	xorpd	%xmm1, %xmm7
#else
	xorpd	%xmm1, %xmm4
	xorpd	%xmm1, %xmm6
#endif

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(RR) || defined(RC) || defined(CR) || defined(CC)
	subpd	%xmm5, %xmm4
	subpd	%xmm7, %xmm6
#else
	addpd	%xmm5, %xmm4
	addpd	%xmm7, %xmm6
#endif

#if defined(LN) || defined(LT)
	movapd	 0 * SIZE(B), %xmm5
	movapd	 2 * SIZE(B), %xmm7

	subpd	%xmm4,  %xmm5
	subpd	%xmm6,  %xmm7
#else
	movapd	 0 * SIZE(AA), %xmm5
	movapd	 2 * SIZE(AA), %xmm7

	subpd	%xmm4,  %xmm5
	subpd	%xmm6,  %xmm7
#endif

#ifndef CONJ
	SHUFPD_1 %xmm1, %xmm1
#endif

#if defined(LN) || defined(LT)
	movlpd	 0 * SIZE(AA), %xmm2
	movhpd	 0 * SIZE(AA), %xmm2
	movlpd	 1 * SIZE(AA), %xmm3
	movhpd	 1 * SIZE(AA), %xmm3

	pshufd	$0x4e, %xmm5, %xmm4
	pshufd	$0x4e, %xmm7, %xmm6

	xorpd	 %xmm1, %xmm4
	xorpd	 %xmm1, %xmm6

	mulpd	 %xmm2, %xmm5
	mulpd	 %xmm3, %xmm4
	mulpd	 %xmm2, %xmm7
	mulpd	 %xmm3, %xmm6

	addpd	 %xmm4, %xmm5
	addpd	 %xmm6, %xmm7
#endif

#ifdef RN
	movlpd	 0 * SIZE(B), %xmm2
	movhpd	 0 * SIZE(B), %xmm2
	movlpd	 1 * SIZE(B), %xmm3
	movhpd	 1 * SIZE(B), %xmm3

	pshufd	$0x4e, %xmm5, %xmm4

	xorpd	 %xmm1, %xmm4

	mulpd	 %xmm2, %xmm5
	mulpd	 %xmm3, %xmm4

	addpd	 %xmm4, %xmm5

	movlpd	 2 * SIZE(B), %xmm2
	movhpd	 2 * SIZE(B), %xmm2
	movlpd	 3 * SIZE(B), %xmm3
	movhpd	 3 * SIZE(B), %xmm3

	movapd	 %xmm5, %xmm4
	pshufd	 $0x4e, %xmm5, %xmm6

	xorpd	 %xmm1, %xmm6

	mulpd	 %xmm2, %xmm4
	mulpd	 %xmm3, %xmm6

	subpd	 %xmm4, %xmm7
	subpd	 %xmm6, %xmm7

	movlpd	 6 * SIZE(B), %xmm2
	movhpd	 6 * SIZE(B), %xmm2
	movlpd	 7 * SIZE(B), %xmm3
	movhpd	 7 * SIZE(B), %xmm3

	pshufd	$0x4e, %xmm7, %xmm6

	xorpd	 %xmm1, %xmm6

	mulpd	 %xmm2, %xmm7
	mulpd	 %xmm3, %xmm6

	addpd	 %xmm6, %xmm7
#endif

#ifdef RT
	movlpd	 6 * SIZE(B), %xmm2
	movhpd	 6 * SIZE(B), %xmm2
	movlpd	 7 * SIZE(B), %xmm3
	movhpd	 7 * SIZE(B), %xmm3

	pshufd	$0x4e, %xmm7, %xmm6

	xorpd	 %xmm1, %xmm6

	mulpd	 %xmm2, %xmm7
	mulpd	 %xmm3, %xmm6

	addpd	 %xmm6, %xmm7

	movlpd	 4 * SIZE(B), %xmm2
	movhpd	 4 * SIZE(B), %xmm2
	movlpd	 5 * SIZE(B), %xmm3
	movhpd	 5 * SIZE(B), %xmm3

	movapd	 %xmm7, %xmm4
	pshufd	 $0x4e, %xmm7, %xmm6

	xorpd	 %xmm1, %xmm6

	mulpd	 %xmm2, %xmm4
	mulpd	 %xmm3, %xmm6

	subpd	 %xmm4, %xmm5
	subpd	 %xmm6, %xmm5

	movlpd	 0 * SIZE(B), %xmm2
	movhpd	 0 * SIZE(B), %xmm2
	movlpd	 1 * SIZE(B), %xmm3
	movhpd	 1 * SIZE(B), %xmm3

	pshufd	$0x4e, %xmm5, %xmm4

	xorpd	 %xmm1, %xmm4

	mulpd	 %xmm2, %xmm5
	mulpd	 %xmm3, %xmm4

	addpd	 %xmm4, %xmm5
#endif

#ifdef LN
	subl	$2 * SIZE, CO1
#endif

	movlpd	%xmm5,   0 * SIZE(CO1)
	movhpd	%xmm5,   1 * SIZE(CO1)

	movlpd	%xmm7,   0 * SIZE(CO1, LDC)
	movhpd	%xmm7,   1 * SIZE(CO1, LDC)

#if defined(LN) || defined(LT)
	movapd	%xmm5,   0 * SIZE(B)
	movapd	%xmm7,   2 * SIZE(B)

	movlpd	%xmm5,   0 * SIZE(BB)
	movlpd	%xmm5,   1 * SIZE(BB)
	movhpd	%xmm5,   2 * SIZE(BB)
	movhpd	%xmm5,   3 * SIZE(BB)
	movlpd	%xmm7,   4 * SIZE(BB)
	movlpd	%xmm7,   5 * SIZE(BB)
	movhpd	%xmm7,   6 * SIZE(BB)
	movhpd	%xmm7,   7 * SIZE(BB)
#else
	movapd	%xmm5,   0 * SIZE(AA)
	movapd	%xmm7,   2 * SIZE(AA)

#endif

#ifndef LN
	addl	$2 * SIZE, CO1
#endif

#if defined(LT) || defined(RN)
	movl	K,  %eax
	subl	KK, %eax
	sall	$ZBASE_SHIFT, %eax
	addl	%eax, AA
#ifdef LT
	addl	$4 * SIZE, B
#endif
#endif

#ifdef LN
	subl	$1, KK
	movl	BORIG, B
#endif

#ifdef LT
	addl	$1, KK
#endif

#ifdef RT
	movl	K, %eax
	movl	BORIG, B
	sall	$ZBASE_SHIFT, %eax
	addl	%eax, AORIG
#endif

	decl	%ebx			# i --
	jg	.L10
	ALIGN_4

.L99:
#ifdef LN
       movl	K, %eax
       sall	$1 + ZBASE_SHIFT, %eax
       addl	%eax, B
#endif

#if defined(LT) || defined(RN)
	movl	K,  %eax
	subl	KK, %eax
	sall	$1 + ZBASE_SHIFT, %eax
	addl	%eax, B
#endif

#ifdef RN
	addl	$2, KK
#endif

#ifdef RT
	subl	$2, KK
#endif

	decl	J			# j --
	jg	.L01
	ALIGN_4

.L100:
	movl	N, %eax
	andl	$1, %eax
	jle	.L500
	ALIGN_4

.L101:
#ifdef LN
	movl	OFFSET, %eax
	addl	M, %eax
	movl	%eax, KK
#endif

	leal	BUFFER, BB

#ifdef RT
       movl	K, %eax
       sall	$ZBASE_SHIFT, %eax
       subl	%eax, B
#endif

#if defined(LN) || defined(RT)
	movl	KK, %eax
	movl	B, BORIG
	sall	$ZBASE_SHIFT, %eax
	addl	%eax, B
	leal	(BB, %eax, 2), BB
#endif

#if defined(LT)
	movl	OFFSET, %eax
	movl	%eax, KK
#endif

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K,  %eax
	subl	KK, %eax
#endif
	sarl	$2, %eax
	jle	.L103
	ALIGN_4

.L102:
	prefetchnta	 56 * SIZE(B)

	movlpd	 0 * SIZE(B), %xmm0
	movlpd	 1 * SIZE(B), %xmm1
	movlpd	 2 * SIZE(B), %xmm2
	movlpd	 3 * SIZE(B), %xmm3
	movlpd	 4 * SIZE(B), %xmm4
	movlpd	 5 * SIZE(B), %xmm5
	movlpd	 6 * SIZE(B), %xmm6
	movlpd	 7 * SIZE(B), %xmm7

	movlpd	%xmm0,  0 * SIZE(BB)
	movlpd	%xmm0,  1 * SIZE(BB)
	movlpd	%xmm1,  2 * SIZE(BB)
	movlpd	%xmm1,  3 * SIZE(BB)
	movlpd	%xmm2,  4 * SIZE(BB)
	movlpd	%xmm2,  5 * SIZE(BB)
	movlpd	%xmm3,  6 * SIZE(BB)
	movlpd	%xmm3,  7 * SIZE(BB)
	movlpd	%xmm4,  8 * SIZE(BB)
	movlpd	%xmm4,  9 * SIZE(BB)
	movlpd	%xmm5, 10 * SIZE(BB)
	movlpd	%xmm5, 11 * SIZE(BB)
	movlpd	%xmm6, 12 * SIZE(BB)
	movlpd	%xmm6, 13 * SIZE(BB)
	movlpd	%xmm7, 14 * SIZE(BB)
	movlpd	%xmm7, 15 * SIZE(BB)

	addl	$  8 * SIZE, B
	subl	$-16 * SIZE, BB
	decl	%eax
	jne	.L102
	ALIGN_4

.L103:
#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	andl	$3, %eax
	BRANCH
	jle	.L105
	ALIGN_4

.L104:
	movlpd	 0 * SIZE(B), %xmm0
	movlpd	 1 * SIZE(B), %xmm1

	movlpd	%xmm0,  0 * SIZE(BB)
	movlpd	%xmm0,  1 * SIZE(BB)
	movlpd	%xmm1,  2 * SIZE(BB)
	movlpd	%xmm1,  3 * SIZE(BB)

	addl	$2 * SIZE, B
	addl	$4 * SIZE, BB
	decl	%eax
	jne	.L104
	ALIGN_4

.L105:
#if defined(LT) || defined(RN)
	movl	A, %eax
	movl	%eax, AA
#else
	movl	A, %eax
	movl	%eax, AORIG
#endif

#ifdef RT
       subl	LDC, C
#endif

	movl	C, CO1

#ifndef RT
	addl	LDC, C
#endif

	movl	M,  %ebx
	testl	%ebx, %ebx
	jle	.L199
	ALIGN_4

.L110:
#ifdef LN
       movl	K, %eax
       sall	$ZBASE_SHIFT, %eax
       subl	%eax, AORIG
#endif

#if defined(LN) || defined(RT)
	movl	AORIG, %eax
	movl	%eax, AA

	movl	KK, %eax
	sall	$ZBASE_SHIFT, %eax
	addl	%eax, AA
#endif

	leal	BUFFER, BB

#if defined(LN) || defined(RT)
	movl	KK, %eax
	sall	$ZBASE_SHIFT, %eax
	leal	(BB, %eax, 2), BB
#endif

	pxor	%xmm4, %xmm4
	pxor	%xmm5, %xmm5
	pxor	%xmm6, %xmm6
	pxor	%xmm7, %xmm7

	movapd	 0 * SIZE(AA), %xmm0
	movapd	 8 * SIZE(AA), %xmm1
	movapd	 0 * SIZE(BB), %xmm2
	movapd	 8 * SIZE(BB), %xmm3

#ifdef LN
	prefetchw -2 * SIZE(CO1)
#else
	prefetchw  2 * SIZE(CO1)
#endif

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	sarl	$3, %eax
	je	.L112
	ALIGN_4

.L111:
	PREFETCH  (PREFETCHSIZE + 0) * SIZE(AA)
	mulpd	%xmm0, %xmm2
	mulpd	 2 * SIZE(BB), %xmm0
	addpd	%xmm2, %xmm4
	movapd	 4 * SIZE(BB), %xmm2
	addpd	%xmm0, %xmm5
	movapd	 2 * SIZE(AA), %xmm0
	mulpd	%xmm0, %xmm2
	mulpd	 6 * SIZE(BB), %xmm0
	addpd	%xmm2, %xmm6
	movapd	16 * SIZE(BB), %xmm2
	addpd	%xmm0, %xmm7
	movapd	 4 * SIZE(AA), %xmm0
	mulpd	%xmm0, %xmm3
	mulpd	10 * SIZE(BB), %xmm0
	addpd	%xmm3, %xmm4
	movapd	12 * SIZE(BB), %xmm3
	addpd	%xmm0, %xmm5
	movapd	 6 * SIZE(AA), %xmm0
	mulpd	%xmm0, %xmm3
	mulpd	14 * SIZE(BB), %xmm0
	addpd	%xmm3, %xmm6
	movapd	24 * SIZE(BB), %xmm3
	addpd	%xmm0, %xmm7
	movapd	16 * SIZE(AA), %xmm0
	mulpd	%xmm1, %xmm2
	mulpd	18 * SIZE(BB), %xmm1
	addpd	%xmm2, %xmm4
	movapd	20 * SIZE(BB), %xmm2
	addpd	%xmm1, %xmm5
	movapd	10 * SIZE(AA), %xmm1
	mulpd	%xmm1, %xmm2
	mulpd	22 * SIZE(BB), %xmm1
	addpd	%xmm2, %xmm6
	movapd	32 * SIZE(BB), %xmm2
	addpd	%xmm1, %xmm7
	movapd	12 * SIZE(AA), %xmm1
	mulpd	%xmm1, %xmm3
	mulpd	26 * SIZE(BB), %xmm1
	addpd	%xmm3, %xmm4
	movapd	28 * SIZE(BB), %xmm3
	addpd	%xmm1, %xmm5
	movapd	14 * SIZE(AA), %xmm1
	mulpd	%xmm1, %xmm3
	mulpd	30 * SIZE(BB), %xmm1
	addpd	%xmm3, %xmm6
	movapd	40 * SIZE(BB), %xmm3
	addpd	%xmm1, %xmm7
	movapd	24 * SIZE(AA), %xmm1

	addl   $16 * SIZE, AA
	addl   $32 * SIZE, BB
	decl   %eax
	jne    .L111
	ALIGN_4

.L112:
#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K,  %eax
	subl	KK, %eax
#endif
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L114
	ALIGN_4

.L113:
	mulpd	%xmm0, %xmm2
	mulpd	 2 * SIZE(BB), %xmm0
	addpd	%xmm2, %xmm4
	movapd	 4 * SIZE(BB), %xmm2
	addpd	%xmm0, %xmm5
	movapd	 2 * SIZE(AA), %xmm0

	addl	$2 * SIZE, AA
	addl	$4 * SIZE, BB
	decl	%eax
	jg	.L113
	ALIGN_4

.L114:
#if defined(LN) || defined(RT)
	movl	KK, %eax
#ifdef LN
	subl	$1, %eax
#else
	subl	$1, %eax
#endif

	movl	AORIG, AA
	movl	BORIG, B
	leal	BUFFER, BB

	sall	$ZBASE_SHIFT, %eax
	addl	%eax, AA
	addl	%eax, B
	leal	(BB, %eax, 2), BB
#endif

	movapd	POSINV,  %xmm1

	addpd	%xmm6, %xmm4
	addpd	%xmm7, %xmm5

	SHUFPD_1 %xmm5, %xmm5

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(NR) || defined(NC) || defined(TR) || defined(TC)
	xorpd	%xmm1, %xmm5
#else
	xorpd	%xmm1, %xmm4
#endif

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(RR) || defined(RC) || defined(CR) || defined(CC)
	subpd	%xmm5, %xmm4
#else
	addpd	%xmm5, %xmm4
#endif

#if defined(LN) || defined(LT)
	movapd	 0 * SIZE(B), %xmm5
	subpd	%xmm4,  %xmm5
#else
	movapd	 0 * SIZE(AA), %xmm5
	subpd	%xmm4,  %xmm5
#endif

#ifndef CONJ
	SHUFPD_1 %xmm1, %xmm1
#endif

#if defined(LN) || defined(LT)
	movlpd	 0 * SIZE(AA), %xmm2
	movhpd	 0 * SIZE(AA), %xmm2
	movlpd	 1 * SIZE(AA), %xmm3
	movhpd	 1 * SIZE(AA), %xmm3

	pshufd	$0x4e, %xmm5, %xmm4
	xorpd	 %xmm1, %xmm4

	mulpd	 %xmm2, %xmm5
	mulpd	 %xmm3, %xmm4

	addpd	 %xmm4, %xmm5
#endif

#ifdef RN
	movlpd	 0 * SIZE(B), %xmm2
	movhpd	 0 * SIZE(B), %xmm2
	movlpd	 1 * SIZE(B), %xmm3
	movhpd	 1 * SIZE(B), %xmm3

	pshufd	$0x4e, %xmm5, %xmm4
	xorpd	 %xmm1, %xmm4

	mulpd	 %xmm2, %xmm5
	mulpd	 %xmm3, %xmm4

	addpd	 %xmm4, %xmm5
#endif

#ifdef RT
	movlpd	 0 * SIZE(B), %xmm2
	movhpd	 0 * SIZE(B), %xmm2
	movlpd	 1 * SIZE(B), %xmm3
	movhpd	 1 * SIZE(B), %xmm3

	pshufd	$0x4e, %xmm5, %xmm4
	xorpd	 %xmm1, %xmm4

	mulpd	 %xmm2, %xmm5
	mulpd	 %xmm3, %xmm4

	addpd	 %xmm4, %xmm5
#endif

#ifdef LN
	subl	$2 * SIZE, CO1
#endif

	movlpd	%xmm5,   0 * SIZE(CO1)
	movhpd	%xmm5,   1 * SIZE(CO1)

#if defined(LN) || defined(LT)
	movapd	%xmm5,   0 * SIZE(B)

	movlpd	%xmm5,   0 * SIZE(BB)
	movlpd	%xmm5,   1 * SIZE(BB)
	movhpd	%xmm5,   2 * SIZE(BB)
	movhpd	%xmm5,   3 * SIZE(BB)
#else
	movapd	%xmm5,   0 * SIZE(AA)
#endif

#ifndef LN
	addl	$2 * SIZE, CO1
#endif

#if defined(LT) || defined(RN)
	movl	K,  %eax
	subl	KK, %eax
	sall	$ZBASE_SHIFT, %eax
	addl	%eax, AA
#ifdef LT
	addl	$2 * SIZE, B
#endif
#endif

#ifdef LN
	subl	$1, KK
	movl	BORIG, B
#endif

#ifdef LT
	addl	$1, KK
#endif

#ifdef RT
	movl	K, %eax
	movl	BORIG, B
	sall	$ZBASE_SHIFT, %eax
	addl	%eax, AORIG
#endif

	decl	%ebx			# i --
	jg	.L110
	ALIGN_4

.L199:
#ifdef LN
       movl	K, %eax
       sall	$ZBASE_SHIFT, %eax
       addl	%eax, B
#endif

#if defined(LT) || defined(RN)
	movl	K,  %eax
	subl	KK, %eax
	sall	$ZBASE_SHIFT, %eax
	addl	%eax, B
#endif

#ifdef RN
	addl	$1, KK
#endif

#ifdef RT
	subl	$1, KK
#endif
	ALIGN_4

.L500:
	movl	OLD_STACK, %esp

	EMMS

	popl	%ebx
	popl	%esi
	popl	%edi
	popl	%ebp
	ret

	EPILOGUE
