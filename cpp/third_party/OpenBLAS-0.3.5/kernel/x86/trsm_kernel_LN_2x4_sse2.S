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

#define OLD_M	 4 + STACK + ARGS(%esi)
#define OLD_N	 8 + STACK + ARGS(%esi)
#define OLD_K	12 + STACK + ARGS(%esi)
#define OLD_ALPHA	16 + STACK + ARGS(%esi)
#define OLD_A	24 + STACK + ARGS(%esi)
#define OLD_B	28 + STACK + ARGS(%esi)
#define OLD_C	32 + STACK + ARGS(%esi)
#define OLD_LDC	36 + STACK + ARGS(%esi)
#define OLD_OFFT	40 + STACK + ARGS(%esi)

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
#define AORIG	56(%esp)
#define BORIG	60(%esp)
#define BUFFER 128(%esp)

#define STACK_ALIGN	4096
#define STACK_OFFSET	1024

#if defined(OPTERON) || defined(BARCELONA) || defined(BOBCAT) || defined(BARCELONA_OPTIMIZATION)
#define PREFETCH     prefetch
#define PREFETCHSIZE (8 * 10 + 4)
#endif

#define B	%edi
#define AA	%edx
#define BB	%ecx
#define LDC	%ebp
#define CO1	%esi

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
	andl	$-STACK_ALIGN, %esp
	addl	$STACK_OFFSET, %esp

	STACK_TOUCHING

	movl	OLD_M, %ebx
	movl	OLD_N, %eax
	movl	OLD_K, %ecx
	movl	OLD_A, %edx

	movl	%ebx, M
	movl	%eax, N
	movl	%ecx, K
	movl	%edx, A
	movl	%esi, OLD_STACK
	movd	OLD_OFFT, %mm4

	movl	OLD_B, B
	movl	OLD_C, %ebx

	movl	%ebx, C
	movl	OLD_LDC, LDC

	movd	%mm4, OFFSET
	movd	%mm4, KK

	leal	(, LDC, SIZE), LDC

#ifdef LN
       movl	M, %eax
       leal	(, %eax, SIZE), %eax
       addl	%eax, C
       imull	K, %eax
       addl	%eax, A
#endif

#ifdef RT
       movl	N, %eax
       leal	(, %eax, SIZE), %eax
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

	movl	N,  %eax
	sarl	$2, %eax
	movl	%eax, J
	jle	.L30
	ALIGN_2

.L01:
#ifdef LN
	movl	OFFSET, %eax
	addl	M, %eax
	movl	%eax, KK
#endif

	leal	BUFFER, BB

#ifdef RT
       movl	K, %eax
       sall	$2 + BASE_SHIFT, %eax
       subl	%eax, B
#endif

#if defined(LN) || defined(RT)
	movl	KK, %eax
	movl	B, BORIG
	leal	(, %eax, SIZE), %eax
	leal	(B,  %eax, 4), B
	leal	(BB, %eax, 8), BB
#endif

#ifdef LT
	movl	OFFSET, %eax
	movl	%eax, KK
#endif

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	sarl	$1, %eax
	jle	.L05
	ALIGN_4

.L02:
#define COPYPREFETCH 40

	prefetchnta	(COPYPREFETCH) * SIZE(B)

	movq	 0 * SIZE(B), %mm0
	movq	 1 * SIZE(B), %mm1
	movq	 2 * SIZE(B), %mm2
	movq	 3 * SIZE(B), %mm3
	movq	 4 * SIZE(B), %mm4
	movq	 5 * SIZE(B), %mm5
	movq	 6 * SIZE(B), %mm6
	movq	 7 * SIZE(B), %mm7

	movq	%mm0,  0 * SIZE(BB)
	movq	%mm0,  1 * SIZE(BB)
	movq	%mm1,  2 * SIZE(BB)
	movq	%mm1,  3 * SIZE(BB)
	movq	%mm2,  4 * SIZE(BB)
	movq	%mm2,  5 * SIZE(BB)
	movq	%mm3,  6 * SIZE(BB)
	movq	%mm3,  7 * SIZE(BB)

	movq	%mm4,  8 * SIZE(BB)
	movq	%mm4,  9 * SIZE(BB)
	movq	%mm5, 10 * SIZE(BB)
	movq	%mm5, 11 * SIZE(BB)
	movq	%mm6, 12 * SIZE(BB)
	movq	%mm6, 13 * SIZE(BB)
	movq	%mm7, 14 * SIZE(BB)
	movq	%mm7, 15 * SIZE(BB)

	addl	$ 8 * SIZE, B
	addl	$16 * SIZE, BB
	decl	%eax
	jne	.L02
	ALIGN_2

.L05:
#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	andl	$1, %eax
	BRANCH
	jle	.L10

	movq	 0 * SIZE(B), %mm0
	movq	 1 * SIZE(B), %mm1
	movq	 2 * SIZE(B), %mm2
	movq	 3 * SIZE(B), %mm3

	movq	%mm0,  0 * SIZE(BB)
	movq	%mm0,  1 * SIZE(BB)
	movq	%mm1,  2 * SIZE(BB)
	movq	%mm1,  3 * SIZE(BB)
	movq	%mm2,  4 * SIZE(BB)
	movq	%mm2,  5 * SIZE(BB)
	movq	%mm3,  6 * SIZE(BB)
	movq	%mm3,  7 * SIZE(BB)

	addl	$4 * SIZE, B
	ALIGN_4

.L10:
#if defined(LT) || defined(RN)
	movl	A, AA
#else
	movl	A, %eax
	movl	%eax, AORIG
#endif

	leal	(, LDC, 4), %eax

#ifdef RT
	subl	%eax, C
#endif
	movl	C, CO1
#ifndef RT
	addl	%eax, C
#endif

	movl	M,  %ebx
	testl	$1, %ebx	# i = (m >> 2)
	jle	.L20

#ifdef LN
       movl	K, %eax
       sall	$BASE_SHIFT, %eax
       subl	%eax, AORIG
#endif

#if defined(LN) || defined(RT)
	movl	KK, %eax
	movl	AORIG, AA
	leal	(AA, %eax, SIZE), AA
#endif

	leal	BUFFER, BB

#if defined(LN) || defined(RT)
	movl	KK, %eax
	sall	$3 + BASE_SHIFT, %eax
	addl	%eax, BB
#endif

	pxor	%xmm4, %xmm4
	pxor	%xmm5, %xmm5
	pxor	%xmm6, %xmm6
	pxor	%xmm7, %xmm7

	movlpd	 0 * SIZE(AA), %xmm0
	movlpd	 4 * SIZE(AA), %xmm1
	movlpd	 0 * SIZE(BB), %xmm2
	movlpd	 8 * SIZE(BB), %xmm3

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	sarl	$3, %eax
	je	.L25
	ALIGN_4

.L22:
	mulsd	%xmm0, %xmm2
	addsd	%xmm2, %xmm4
#if defined(OPTERON) || defined(BARCELONA) || defined(BOBCAT) || defined(BARCELONA_OPTIMIZATION)
	PREFETCH (PREFETCHSIZE  + 0) * SIZE(AA)
#endif
	movlpd	 2 * SIZE(BB), %xmm2
	mulsd	%xmm0, %xmm2
	addsd	%xmm2, %xmm5
	movlpd	 4 * SIZE(BB), %xmm2
	mulsd	%xmm0, %xmm2
	mulsd	 6 * SIZE(BB), %xmm0
	addsd	%xmm2, %xmm6
	movlpd	16 * SIZE(BB), %xmm2
	addsd	%xmm0, %xmm7
	movlpd	 1 * SIZE(AA), %xmm0
	mulsd	%xmm0, %xmm3
	addsd	%xmm3, %xmm4
	movlpd	10 * SIZE(BB), %xmm3
	mulsd	%xmm0, %xmm3
	addsd	%xmm3, %xmm5
	movlpd	12 * SIZE(BB), %xmm3
	mulsd	%xmm0, %xmm3
	mulsd	14 * SIZE(BB), %xmm0
	addsd	%xmm3, %xmm6
	movlpd	24 * SIZE(BB), %xmm3
	addsd	%xmm0, %xmm7
	movlpd	 2 * SIZE(AA), %xmm0
	mulsd	%xmm0, %xmm2
	addsd	%xmm2, %xmm4
	movlpd	18 * SIZE(BB), %xmm2
	mulsd	%xmm0, %xmm2
	addsd	%xmm2, %xmm5
	movlpd	20 * SIZE(BB), %xmm2
	mulsd	%xmm0, %xmm2
	mulsd	22 * SIZE(BB), %xmm0
	addsd	%xmm2, %xmm6
	movlpd	32 * SIZE(BB), %xmm2
	addsd	%xmm0, %xmm7
	movlpd	 3 * SIZE(AA), %xmm0
	mulsd	%xmm0, %xmm3
	addsd	%xmm3, %xmm4
	movlpd	26 * SIZE(BB), %xmm3
	mulsd	%xmm0, %xmm3
	addsd	%xmm3, %xmm5
	movlpd	28 * SIZE(BB), %xmm3
	mulsd	%xmm0, %xmm3
	mulsd	30 * SIZE(BB), %xmm0
	addsd	%xmm3, %xmm6
	movlpd	40 * SIZE(BB), %xmm3
	addsd	%xmm0, %xmm7
	movlpd	 8 * SIZE(AA), %xmm0
#if defined(OPTERON) || defined(BARCELONA) || defined(BOBCAT) || defined(BARCELONA_OPTIMIZATION)
	PREFETCH (PREFETCHSIZE  + 8) * SIZE(AA)
#endif
	mulsd	%xmm1, %xmm2
	addsd	%xmm2, %xmm4
	movlpd	34 * SIZE(BB), %xmm2
	mulsd	%xmm1, %xmm2
	addsd	%xmm2, %xmm5
	movlpd	36 * SIZE(BB), %xmm2
	mulsd	%xmm1, %xmm2
	mulsd	38 * SIZE(BB), %xmm1
	addsd	%xmm2, %xmm6
	movlpd	48 * SIZE(BB), %xmm2
	addsd	%xmm1, %xmm7
	movlpd	 5 * SIZE(AA), %xmm1
	mulsd	%xmm1, %xmm3
	addsd	%xmm3, %xmm4
	movlpd	42 * SIZE(BB), %xmm3
	mulsd	%xmm1, %xmm3
	addsd	%xmm3, %xmm5
	movlpd	44 * SIZE(BB), %xmm3
	mulsd	%xmm1, %xmm3
	mulsd	46 * SIZE(BB), %xmm1
	addsd	%xmm3, %xmm6
	movlpd	56 * SIZE(BB), %xmm3
	addsd	%xmm1, %xmm7
	movlpd	 6 * SIZE(AA), %xmm1
	mulsd	%xmm1, %xmm2
	addsd	%xmm2, %xmm4
	movlpd	50 * SIZE(BB), %xmm2
	mulsd	%xmm1, %xmm2
	addsd	%xmm2, %xmm5
	movlpd	52 * SIZE(BB), %xmm2
	mulsd	%xmm1, %xmm2
	mulsd	54 * SIZE(BB), %xmm1
	addsd	%xmm2, %xmm6
	movlpd	64 * SIZE(BB), %xmm2
	addsd	%xmm1, %xmm7
	movlpd	 7 * SIZE(AA), %xmm1
	mulsd	%xmm1, %xmm3
	addsd	%xmm3, %xmm4
	movlpd	58 * SIZE(BB), %xmm3
	mulsd	%xmm1, %xmm3
	addsd	%xmm3, %xmm5
	movlpd	60 * SIZE(BB), %xmm3
	mulsd	%xmm1, %xmm3
	mulsd	62 * SIZE(BB), %xmm1
	addsd	%xmm3, %xmm6
	movlpd	72 * SIZE(BB), %xmm3
	addl   $64 * SIZE, BB
	addsd	%xmm1, %xmm7
	movlpd	12 * SIZE(AA), %xmm1
	addl   $8 * SIZE, AA
	decl   %eax
	jne    .L22
	ALIGN_4

.L25:
#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L28

.L26:
	mulsd	%xmm0, %xmm2
	addsd	%xmm2, %xmm4
	movlpd	 2 * SIZE(BB), %xmm2
	mulsd	%xmm0, %xmm2
	addsd	%xmm2, %xmm5
	movlpd	 4 * SIZE(BB), %xmm2
	mulsd	%xmm0, %xmm2
	mulsd	 6 * SIZE(BB), %xmm0
	addsd	%xmm2, %xmm6
	movlpd	 8 * SIZE(BB), %xmm2
	addsd	%xmm0, %xmm7
	movlpd	 1 * SIZE(AA), %xmm0

	addl	$1 * SIZE, AA
	addl	$8 * SIZE, BB
	decl	%eax
	jg	.L26
	ALIGN_4

.L28:
#if defined(LN) || defined(RT)
	movl	KK, %eax
#ifdef LN
	subl	$1, %eax
#else
	subl	$4, %eax
#endif

	movl	AORIG, AA
	movl	BORIG, B
	leal	BUFFER, BB

	leal	(, %eax, SIZE), %eax
	addl	%eax, AA
	leal	(B,  %eax, 4), B
	leal	(BB, %eax, 8), BB
#endif

#if defined(LN) || defined(LT)
	unpcklpd %xmm5, %xmm4
	unpcklpd %xmm7, %xmm6

	movapd	 0 * SIZE(B), %xmm2
	movapd	 2 * SIZE(B), %xmm5

	subpd	%xmm4,  %xmm2
	subpd	%xmm6,  %xmm5
#else
	movlpd	 0 * SIZE(AA), %xmm0
	movlpd	 1 * SIZE(AA), %xmm1
	movlpd	 2 * SIZE(AA), %xmm2
	movlpd	 3 * SIZE(AA), %xmm3

	subsd	%xmm4, %xmm0
	subsd	%xmm5, %xmm1
	subsd	%xmm6, %xmm2
	subsd	%xmm7, %xmm3
#endif

#ifdef LN
	movlpd	 0 * SIZE(AA), %xmm4
	movhpd	 0 * SIZE(AA), %xmm4
	mulpd	 %xmm4, %xmm2
	mulpd	 %xmm4, %xmm5
#endif

#ifdef LT
	movlpd	 0 * SIZE(AA), %xmm4
	movhpd	 0 * SIZE(AA), %xmm4
	mulpd	 %xmm4, %xmm2
	mulpd	 %xmm4, %xmm5
#endif

#ifdef RN
	movlpd	 0 * SIZE(B), %xmm4
	mulsd	 %xmm4, %xmm0
	movlpd	 1 * SIZE(B), %xmm4
	mulsd	 %xmm0, %xmm4
	subsd	 %xmm4, %xmm1
	movlpd	 2 * SIZE(B), %xmm4
	mulsd	 %xmm0, %xmm4
	subsd	 %xmm4, %xmm2
	movlpd	 3 * SIZE(B), %xmm4
	mulsd	 %xmm0, %xmm4
	subsd	 %xmm4, %xmm3

	movlpd	 5 * SIZE(B), %xmm4
	mulsd	 %xmm4, %xmm1
	movlpd	 6 * SIZE(B), %xmm4
	mulsd	 %xmm1, %xmm4
	subsd	 %xmm4, %xmm2
	movlpd	 7 * SIZE(B), %xmm4
	mulsd	 %xmm1, %xmm4
	subsd	 %xmm4, %xmm3

	movlpd	10 * SIZE(B), %xmm4
	mulsd	 %xmm4, %xmm2
	movlpd	11 * SIZE(B), %xmm4
	mulsd	 %xmm2, %xmm4
	subsd	 %xmm4, %xmm3

	movlpd	15 * SIZE(B), %xmm4
	mulsd	 %xmm4, %xmm3
#endif

#ifdef RT
	movlpd	15 * SIZE(B), %xmm4
	mulsd	 %xmm4, %xmm3
	movlpd	14 * SIZE(B), %xmm4
	mulsd	 %xmm3, %xmm4
	subsd	 %xmm4, %xmm2
	movlpd	13 * SIZE(B), %xmm4
	mulsd	 %xmm3, %xmm4
	subsd	 %xmm4, %xmm1
	movlpd	12 * SIZE(B), %xmm4
	mulsd	 %xmm3, %xmm4
	subsd	 %xmm4, %xmm0

	movlpd	10 * SIZE(B), %xmm4
	mulsd	 %xmm4, %xmm2
	movlpd	 9 * SIZE(B), %xmm4
	mulsd	 %xmm2, %xmm4
	subsd	 %xmm4, %xmm1
	movlpd	 8 * SIZE(B), %xmm4
	mulsd	 %xmm2, %xmm4
	subsd	 %xmm4, %xmm0

	movlpd	 5 * SIZE(B), %xmm4
	mulsd	 %xmm4, %xmm1
	movlpd	 4 * SIZE(B), %xmm4
	mulsd	 %xmm1, %xmm4
	subsd	 %xmm4, %xmm0

	movlpd	 0 * SIZE(B), %xmm4
	mulsd	 %xmm4, %xmm0
#endif

#if defined(LN) || defined(LT)
	movapd	%xmm2,   0 * SIZE(B)
	movapd	%xmm5,   2 * SIZE(B)

	movlpd	%xmm2,   0 * SIZE(BB)
	movlpd	%xmm2,   1 * SIZE(BB)
	movhpd	%xmm2,   2 * SIZE(BB)
	movhpd	%xmm2,   3 * SIZE(BB)
	movlpd	%xmm5,   4 * SIZE(BB)
	movlpd	%xmm5,   5 * SIZE(BB)
	movhpd	%xmm5,   6 * SIZE(BB)
	movhpd	%xmm5,   7 * SIZE(BB)
#else
	movlpd	%xmm0,   0 * SIZE(AA)
	movlpd	%xmm1,   1 * SIZE(AA)
	movlpd	%xmm2,   2 * SIZE(AA)
	movlpd	%xmm3,   3 * SIZE(AA)
#endif

#ifdef LN
	subl	$1 * SIZE, CO1
#endif

	leal	(LDC, LDC, 2), %eax

#if defined(LN) || defined(LT)
	movlpd	%xmm2, 0 * SIZE(CO1)
	movhpd	%xmm2, 0 * SIZE(CO1, LDC, 1)
	movlpd	%xmm5, 0 * SIZE(CO1, LDC, 2)
	movhpd	%xmm5, 0 * SIZE(CO1, %eax, 1)
#else
	movlpd	%xmm0, 0 * SIZE(CO1)
	movlpd	%xmm1, 0 * SIZE(CO1, LDC, 1)
	movlpd	%xmm2, 0 * SIZE(CO1, LDC, 2)
	movlpd	%xmm3, 0 * SIZE(CO1, %eax, 1)
#endif

#ifndef LN
	addl	$1 * SIZE, CO1
#endif

#if defined(LT) || defined(RN)
	movl	K,  %eax
	subl	KK, %eax
	leal	(AA,%eax, SIZE), AA
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
	sall	$BASE_SHIFT, %eax
	addl	%eax, AORIG
#endif
	ALIGN_4

.L20:
	movl	M,  %ebx
	sarl	$1, %ebx	# i = (m >> 2)
	jle	.L29
	ALIGN_4

.L11:
#ifdef LN
       movl	K, %eax
       sall	$1 + BASE_SHIFT, %eax
       subl	%eax, AORIG
#endif

#if defined(LN) || defined(RT)
	movl	KK, %eax
	movl	AORIG, AA
	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 2), AA
#endif

	leal	BUFFER, BB

#if defined(LN) || defined(RT)
	movl	KK, %eax
	sall	$3 + BASE_SHIFT, %eax
	addl	%eax, BB
#endif

	pxor	%xmm4, %xmm4
	pxor	%xmm5, %xmm5
	pxor	%xmm6, %xmm6
	pxor	%xmm7, %xmm7

	movapd	 0 * SIZE(AA), %xmm0
	movapd	 8 * SIZE(AA), %xmm1
	movapd	 0 * SIZE(BB), %xmm2
	movapd	 8 * SIZE(BB), %xmm3

	leal	(LDC, LDC, 2), %eax

#ifdef LN
	prefetchw	-2 * SIZE(CO1)
	prefetchw	-2 * SIZE(CO1, LDC)
	prefetchw	-2 * SIZE(CO1, LDC, 2)
	prefetchw	-2 * SIZE(CO1, %eax)
#else
	prefetchw	 1 * SIZE(CO1)
	prefetchw	 1 * SIZE(CO1, LDC)
	prefetchw	 1 * SIZE(CO1, LDC, 2)
	prefetchw	 1 * SIZE(CO1, %eax)
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
	jne    .L12
	ALIGN_4
#endif

.L15:
#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L18
	ALIGN_3

.L16:
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
	jg	.L16
	ALIGN_4

.L18:
#if defined(LN) || defined(RT)
	movl	KK, %eax
#ifdef LN
	subl	$2, %eax
#else
	subl	$4, %eax
#endif

	movl	AORIG, AA
	movl	BORIG, B
	leal	BUFFER, BB

	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 2), AA
	leal	(B,  %eax, 4), B
	leal	(BB, %eax, 8), BB
#endif

#if defined(LN) || defined(LT)
	movapd	 %xmm4, %xmm0
	unpcklpd %xmm5, %xmm4
	unpckhpd %xmm5, %xmm0

	movapd	 %xmm6, %xmm1
	unpcklpd %xmm7, %xmm6
	unpckhpd %xmm7, %xmm1

	movapd	 0 * SIZE(B), %xmm2
	movapd	 2 * SIZE(B), %xmm5
	movapd	 4 * SIZE(B), %xmm3
	movapd	 6 * SIZE(B), %xmm7

	subpd	%xmm4,  %xmm2
	subpd	%xmm6,  %xmm5
	subpd	%xmm0,  %xmm3
	subpd	%xmm1,  %xmm7
#else
	movapd	 0 * SIZE(AA), %xmm0
	movapd	 2 * SIZE(AA), %xmm1
	movapd	 4 * SIZE(AA), %xmm2
	movapd	 6 * SIZE(AA), %xmm3

	subpd	%xmm4, %xmm0
	subpd	%xmm5, %xmm1
	subpd	%xmm6, %xmm2
	subpd	%xmm7, %xmm3
#endif

#ifdef LN
	movlpd	 3 * SIZE(AA), %xmm4
	movhpd	 3 * SIZE(AA), %xmm4
	mulpd	 %xmm4, %xmm3
	mulpd	 %xmm4, %xmm7

	movlpd	 2 * SIZE(AA), %xmm4
	movhpd	 2 * SIZE(AA), %xmm4
	movapd	 %xmm4, %xmm6
	mulpd	 %xmm3, %xmm4
	subpd	 %xmm4, %xmm2
	mulpd	 %xmm7, %xmm6
	subpd	 %xmm6, %xmm5

	movlpd	 0 * SIZE(AA), %xmm4
	movhpd	 0 * SIZE(AA), %xmm4
	mulpd	 %xmm4, %xmm2
	mulpd	 %xmm4, %xmm5

#endif

#ifdef LT
	movlpd	 0 * SIZE(AA), %xmm4
	movhpd	 0 * SIZE(AA), %xmm4
	mulpd	 %xmm4, %xmm2
	mulpd	 %xmm4, %xmm5

	movlpd	 1 * SIZE(AA), %xmm4
	movhpd	 1 * SIZE(AA), %xmm4
	movapd	 %xmm4, %xmm6
	mulpd	 %xmm2, %xmm4
	subpd	 %xmm4, %xmm3
	mulpd	 %xmm5, %xmm6
	subpd	 %xmm6, %xmm7

	movlpd	 3 * SIZE(AA), %xmm4
	movhpd	 3 * SIZE(AA), %xmm4
	mulpd	 %xmm4, %xmm3
	mulpd	 %xmm4, %xmm7
#endif

#ifdef RN
	movlpd	 0 * SIZE(B), %xmm4
	movhpd	 0 * SIZE(B), %xmm4
	mulpd	 %xmm4, %xmm0
	movlpd	 1 * SIZE(B), %xmm4
	movhpd	 1 * SIZE(B), %xmm4
	mulpd	 %xmm0, %xmm4
	subpd	 %xmm4, %xmm1
	movlpd	 2 * SIZE(B), %xmm4
	movhpd	 2 * SIZE(B), %xmm4
	mulpd	 %xmm0, %xmm4
	subpd	 %xmm4, %xmm2
	movlpd	 3 * SIZE(B), %xmm4
	movhpd	 3 * SIZE(B), %xmm4
	mulpd	 %xmm0, %xmm4
	subpd	 %xmm4, %xmm3

	movlpd	 5 * SIZE(B), %xmm4
	movhpd	 5 * SIZE(B), %xmm4
	mulpd	 %xmm4, %xmm1
	movlpd	 6 * SIZE(B), %xmm4
	movhpd	 6 * SIZE(B), %xmm4
	mulpd	 %xmm1, %xmm4
	subpd	 %xmm4, %xmm2
	movlpd	 7 * SIZE(B), %xmm4
	movhpd	 7 * SIZE(B), %xmm4
	mulpd	 %xmm1, %xmm4
	subpd	 %xmm4, %xmm3

	movlpd	10 * SIZE(B), %xmm4
	movhpd	10 * SIZE(B), %xmm4
	mulpd	 %xmm4, %xmm2
	movlpd	11 * SIZE(B), %xmm4
	movhpd	11 * SIZE(B), %xmm4
	mulpd	 %xmm2, %xmm4
	subpd	 %xmm4, %xmm3

	movlpd	15 * SIZE(B), %xmm4
	movhpd	15 * SIZE(B), %xmm4
	mulpd	 %xmm4, %xmm3
#endif

#ifdef RT
	movlpd	15 * SIZE(B), %xmm4
	movhpd	15 * SIZE(B), %xmm4
	mulpd	 %xmm4, %xmm3
	movlpd	14 * SIZE(B), %xmm4
	movhpd	14 * SIZE(B), %xmm4
	mulpd	 %xmm3, %xmm4
	subpd	 %xmm4, %xmm2
	movlpd	13 * SIZE(B), %xmm4
	movhpd	13 * SIZE(B), %xmm4
	mulpd	 %xmm3, %xmm4
	subpd	 %xmm4, %xmm1
	movlpd	12 * SIZE(B), %xmm4
	movhpd	12 * SIZE(B), %xmm4
	mulpd	 %xmm3, %xmm4
	subpd	 %xmm4, %xmm0

	movlpd	10 * SIZE(B), %xmm4
	movhpd	10 * SIZE(B), %xmm4
	mulpd	 %xmm4, %xmm2
	movlpd	 9 * SIZE(B), %xmm4
	movhpd	 9 * SIZE(B), %xmm4
	mulpd	 %xmm2, %xmm4
	subpd	 %xmm4, %xmm1
	movlpd	 8 * SIZE(B), %xmm4
	movhpd	 8 * SIZE(B), %xmm4
	mulpd	 %xmm2, %xmm4
	subpd	 %xmm4, %xmm0

	movlpd	 5 * SIZE(B), %xmm4
	movhpd	 5 * SIZE(B), %xmm4
	mulpd	 %xmm4, %xmm1
	movlpd	 4 * SIZE(B), %xmm4
	movhpd	 4 * SIZE(B), %xmm4
	mulpd	 %xmm1, %xmm4
	subpd	 %xmm4, %xmm0

	movlpd	 0 * SIZE(B), %xmm4
	movhpd	 0 * SIZE(B), %xmm4
	mulpd	 %xmm4, %xmm0
#endif

#if defined(LN) || defined(LT)
	movapd	%xmm2,   0 * SIZE(B)
	movapd	%xmm5,   2 * SIZE(B)
	movapd	%xmm3,   4 * SIZE(B)
	movapd	%xmm7,   6 * SIZE(B)

	movlpd	%xmm2,   0 * SIZE(BB)
	movlpd	%xmm2,   1 * SIZE(BB)
	movhpd	%xmm2,   2 * SIZE(BB)
	movhpd	%xmm2,   3 * SIZE(BB)
	movlpd	%xmm5,   4 * SIZE(BB)
	movlpd	%xmm5,   5 * SIZE(BB)
	movhpd	%xmm5,   6 * SIZE(BB)
	movhpd	%xmm5,   7 * SIZE(BB)
	movlpd	%xmm3,   8 * SIZE(BB)
	movlpd	%xmm3,   9 * SIZE(BB)
	movhpd	%xmm3,  10 * SIZE(BB)
	movhpd	%xmm3,  11 * SIZE(BB)
	movlpd	%xmm7,  12 * SIZE(BB)
	movlpd	%xmm7,  13 * SIZE(BB)
	movhpd	%xmm7,  14 * SIZE(BB)
	movhpd	%xmm7,  15 * SIZE(BB)
#else
	movapd	%xmm0,   0 * SIZE(AA)
	movapd	%xmm1,   2 * SIZE(AA)
	movapd	%xmm2,   4 * SIZE(AA)
	movapd	%xmm3,   6 * SIZE(AA)
#endif

#ifdef LN
	subl	$2 * SIZE, CO1
#endif

	leal	(LDC, LDC, 2), %eax

#if defined(LN) || defined(LT)
	movlpd	%xmm2, 0 * SIZE(CO1)
	movlpd	%xmm3, 1 * SIZE(CO1)
	movhpd	%xmm2, 0 * SIZE(CO1, LDC, 1)
	movhpd	%xmm3, 1 * SIZE(CO1, LDC, 1)
	movlpd	%xmm5, 0 * SIZE(CO1, LDC, 2)
	movlpd	%xmm7, 1 * SIZE(CO1, LDC, 2)
	movhpd	%xmm5, 0 * SIZE(CO1, %eax, 1)
	movhpd	%xmm7, 1 * SIZE(CO1, %eax, 1)
#else
	movlpd	%xmm0, 0 * SIZE(CO1)
	movhpd	%xmm0, 1 * SIZE(CO1)
	movlpd	%xmm1, 0 * SIZE(CO1, LDC, 1)
	movhpd	%xmm1, 1 * SIZE(CO1, LDC, 1)
	movlpd	%xmm2, 0 * SIZE(CO1, LDC, 2)
	movhpd	%xmm2, 1 * SIZE(CO1, LDC, 2)
	movlpd	%xmm3, 0 * SIZE(CO1, %eax, 1)
	movhpd	%xmm3, 1 * SIZE(CO1, %eax, 1)
#endif

#ifndef LN
	addl	$2 * SIZE, CO1
#endif

#if defined(LT) || defined(RN)
	movl	K,  %eax
	subl	KK, %eax
	leal	(,%eax, SIZE), %eax
	leal	(AA, %eax, 2), AA
#ifdef LT
	addl	$8 * SIZE, B
#endif
#endif

#ifdef LN
	subl	$2, KK
	movl	BORIG, B
#endif

#ifdef LT
	addl	$2, KK
#endif

#ifdef RT
	movl	K, %eax
	movl	BORIG, B
	sall	$1 + BASE_SHIFT, %eax
	addl	%eax, AORIG
#endif

	decl	%ebx			# i --
	jg	.L11
	ALIGN_4

.L29:
#ifdef LN
       movl	K, %eax
       leal	(, %eax, SIZE), %eax
       leal 	(B, %eax, 4), B
#endif

#if defined(LT) || defined(RN)
	movl	K,  %eax
	subl	KK, %eax
	leal	(,%eax, SIZE), %eax
	leal	(B,  %eax, 4), B
#endif

#ifdef RN
	addl	$4, KK
#endif

#ifdef RT
	subl	$4, KK
#endif

	decl	J			# j --
	jg	.L01
	ALIGN_4

.L30:
	testl	$2, N
	je	.L60

#ifdef LN
	movl	OFFSET, %eax
	addl	M, %eax
	movl	%eax, KK
#endif

	leal	BUFFER, BB

#ifdef RT
       movl	K, %eax
       sall	$1 + BASE_SHIFT, %eax
       subl	%eax, B
#endif

#if defined(LN) || defined(RT)
	movl	KK, %eax
	movl	B, BORIG
	leal	(, %eax, SIZE), %eax
	leal	(B,  %eax, 2), B
	leal	(BB, %eax, 4), BB
#endif

#ifdef LT
	movl	OFFSET, %eax
	movl	%eax, KK
#endif

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	sarl	$2, %eax
	jle	.L35
	ALIGN_4

.L32:
#define COPYPREFETCH 40

	prefetchnta	(COPYPREFETCH) * SIZE(B)

	movq	 0 * SIZE(B), %mm0
	movq	 1 * SIZE(B), %mm1
	movq	 2 * SIZE(B), %mm2
	movq	 3 * SIZE(B), %mm3
	movq	 4 * SIZE(B), %mm4
	movq	 5 * SIZE(B), %mm5
	movq	 6 * SIZE(B), %mm6
	movq	 7 * SIZE(B), %mm7

	movq	%mm0,  0 * SIZE(BB)
	movq	%mm0,  1 * SIZE(BB)
	movq	%mm1,  2 * SIZE(BB)
	movq	%mm1,  3 * SIZE(BB)
	movq	%mm2,  4 * SIZE(BB)
	movq	%mm2,  5 * SIZE(BB)
	movq	%mm3,  6 * SIZE(BB)
	movq	%mm3,  7 * SIZE(BB)

	movq	%mm4,  8 * SIZE(BB)
	movq	%mm4,  9 * SIZE(BB)
	movq	%mm5, 10 * SIZE(BB)
	movq	%mm5, 11 * SIZE(BB)
	movq	%mm6, 12 * SIZE(BB)
	movq	%mm6, 13 * SIZE(BB)
	movq	%mm7, 14 * SIZE(BB)
	movq	%mm7, 15 * SIZE(BB)

	addl	$ 8 * SIZE, B
	addl	$16 * SIZE, BB
	decl	%eax
	jne	.L32
	ALIGN_2

.L35:
#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	andl	$3, %eax
	BRANCH
	jle	.L40
	ALIGN_2

.L36:
	movq	 0 * SIZE(B), %mm0
	movq	 1 * SIZE(B), %mm1

	movq	%mm0,  0 * SIZE(BB)
	movq	%mm0,  1 * SIZE(BB)
	movq	%mm1,  2 * SIZE(BB)
	movq	%mm1,  3 * SIZE(BB)

	addl	$2 * SIZE, B
	addl	$4 * SIZE, BB
	decl	%eax
	jne	.L36
	ALIGN_4

.L40:
#if defined(LT) || defined(RN)
	movl	A, AA
#else
	movl	A, %eax
	movl	%eax, AORIG
#endif

	leal	(, LDC, 2), %eax

#ifdef RT
	subl	%eax, C
#endif
	movl	C, CO1
#ifndef RT
	addl	%eax, C
#endif

	movl	M,  %ebx
	testl	$1, %ebx	# i = (m >> 2)
	jle	.L50

#ifdef LN
       movl	K, %eax
       sall	$BASE_SHIFT, %eax
       subl	%eax, AORIG
#endif

#if defined(LN) || defined(RT)
	movl	KK, %eax
	movl	AORIG, AA
	leal	(AA, %eax, SIZE), AA
#endif

	leal	BUFFER, BB

#if defined(LN) || defined(RT)
	movl	KK, %eax
	sall	$2 + BASE_SHIFT, %eax
	addl	%eax, BB
#endif

	pxor	%xmm4, %xmm4
	pxor	%xmm5, %xmm5
	pxor	%xmm6, %xmm6
	pxor	%xmm7, %xmm7

	movlpd	 0 * SIZE(AA), %xmm0
	movlpd	 4 * SIZE(AA), %xmm1
	movlpd	 0 * SIZE(BB), %xmm2
	movlpd	 8 * SIZE(BB), %xmm3

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	sarl	$3, %eax
	je	.L55
	ALIGN_4

.L52:
	mulsd	%xmm0, %xmm2
	PREFETCH (PREFETCHSIZE  + 0) * SIZE(AA)
	mulsd	 2 * SIZE(BB), %xmm0
	addsd	%xmm2, %xmm4
	movlpd	 4 * SIZE(BB), %xmm2
	addsd	%xmm0, %xmm5
	movlpd	 1 * SIZE(AA), %xmm0

	mulsd	%xmm0, %xmm2
	mulsd	 6 * SIZE(BB), %xmm0
	addsd	%xmm2, %xmm6
	movlpd	16 * SIZE(BB), %xmm2
	addsd	%xmm0, %xmm7
	movlpd	 2 * SIZE(AA), %xmm0

	mulsd	%xmm0, %xmm3
	mulsd	10 * SIZE(BB), %xmm0
	addsd	%xmm3, %xmm4
	movlpd	12 * SIZE(BB), %xmm3
	addsd	%xmm0, %xmm5
	movlpd	 3 * SIZE(AA), %xmm0

	mulsd	%xmm0, %xmm3
	mulsd	14 * SIZE(BB), %xmm0
	addsd	%xmm3, %xmm6
	movlpd	24 * SIZE(BB), %xmm3
	addsd	%xmm0, %xmm7
	movlpd	 8 * SIZE(AA), %xmm0

	mulsd	%xmm1, %xmm2
	mulsd	18 * SIZE(BB), %xmm1
	addsd	%xmm2, %xmm4
	movlpd	20 * SIZE(BB), %xmm2
	addsd	%xmm1, %xmm5
	movlpd	 5 * SIZE(AA), %xmm1

	mulsd	%xmm1, %xmm2
	mulsd	22 * SIZE(BB), %xmm1
	addsd	%xmm2, %xmm6
	movlpd	32 * SIZE(BB), %xmm2
	addsd	%xmm1, %xmm7
	movlpd	 6 * SIZE(AA), %xmm1

	mulsd	%xmm1, %xmm3
	mulsd	26 * SIZE(BB), %xmm1
	addsd	%xmm3, %xmm4
	movlpd	28 * SIZE(BB), %xmm3
	addsd	%xmm1, %xmm5
	movlpd	 7 * SIZE(AA), %xmm1

	mulsd	%xmm1, %xmm3
	mulsd	30 * SIZE(BB), %xmm1
	addsd	%xmm3, %xmm6
	movlpd	40 * SIZE(BB), %xmm3
	addsd	%xmm1, %xmm7
	movlpd	12 * SIZE(AA), %xmm1

	addl   $ 8 * SIZE, AA
	addl   $32 * SIZE, BB
	decl   %eax
	jne    .L52
	ALIGN_4

.L55:
#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L58

.L56:
	mulsd	%xmm0, %xmm2
	mulsd	 2 * SIZE(BB), %xmm0
	addsd	%xmm2, %xmm4
	movlpd	 4 * SIZE(BB), %xmm2
	addsd	%xmm0, %xmm5
	movlpd	 1 * SIZE(AA), %xmm0

	addl	$1 * SIZE, AA
	addl	$4 * SIZE, BB
	decl	%eax
	jg	.L56
	ALIGN_4

.L58:
	addsd	%xmm6, %xmm4
	addsd	%xmm7, %xmm5

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

	leal	(, %eax, SIZE), %eax
	addl	%eax, AA
	leal	(B,  %eax, 2), B
	leal	(BB, %eax, 4), BB
#endif

#if defined(LN) || defined(LT)
	unpcklpd %xmm5, %xmm4

	movapd	 0 * SIZE(B), %xmm2

	subpd	%xmm4,  %xmm2
#else
	movlpd	 0 * SIZE(AA), %xmm0
	movlpd	 1 * SIZE(AA), %xmm1

	subsd	%xmm4, %xmm0
	subsd	%xmm5, %xmm1
#endif

#ifdef LN
	movlpd	 0 * SIZE(AA), %xmm4
	movhpd	 0 * SIZE(AA), %xmm4
	mulpd	 %xmm4, %xmm2
#endif

#ifdef LT
	movlpd	 0 * SIZE(AA), %xmm4
	movhpd	 0 * SIZE(AA), %xmm4
	mulpd	 %xmm4, %xmm2
#endif

#ifdef RN
	movlpd	 0 * SIZE(B), %xmm4
	mulsd	 %xmm4, %xmm0
	movlpd	 1 * SIZE(B), %xmm4
	mulsd	 %xmm0, %xmm4
	subsd	 %xmm4, %xmm1

	movlpd	 3 * SIZE(B), %xmm4
	mulsd	 %xmm4, %xmm1
#endif

#ifdef RT
	movlpd	 3 * SIZE(B), %xmm4
	mulsd	 %xmm4, %xmm1
	movlpd	 2 * SIZE(B), %xmm4
	mulsd	 %xmm1, %xmm4
	subsd	 %xmm4, %xmm0

	movlpd	 0 * SIZE(B), %xmm4
	mulsd	 %xmm4, %xmm0
#endif

#if defined(LN) || defined(LT)
	movapd	%xmm2,   0 * SIZE(B)

	movlpd	%xmm2,   0 * SIZE(BB)
	movlpd	%xmm2,   1 * SIZE(BB)
	movhpd	%xmm2,   2 * SIZE(BB)
	movhpd	%xmm2,   3 * SIZE(BB)
#else
	movlpd	%xmm0,   0 * SIZE(AA)
	movlpd	%xmm1,   1 * SIZE(AA)
#endif

#ifdef LN
	subl	$1 * SIZE, CO1
#endif

#if defined(LN) || defined(LT)
	movlpd	%xmm2, 0 * SIZE(CO1)
	movhpd	%xmm2, 0 * SIZE(CO1, LDC, 1)
#else
	movlpd	%xmm0, 0 * SIZE(CO1)
	movlpd	%xmm1, 0 * SIZE(CO1, LDC, 1)
#endif

#ifndef LN
	addl	$1 * SIZE, CO1
#endif

#if defined(LT) || defined(RN)
	movl	K,  %eax
	subl	KK, %eax
	leal	(AA,%eax, SIZE), AA
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
	sall	$BASE_SHIFT, %eax
	addl	%eax, AORIG
#endif
	ALIGN_4

.L50:
	movl	M,  %ebx
	sarl	$1, %ebx	# i = (m >> 2)
	jle	.L59
	ALIGN_4

.L41:
#ifdef LN
       movl	K, %eax
       sall	$1 + BASE_SHIFT, %eax
       subl	%eax, AORIG
#endif

#if defined(LN) || defined(RT)
	movl	KK, %eax
	movl	AORIG, AA
	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 2), AA
#endif

	leal	BUFFER, BB

#if defined(LN) || defined(RT)
	movl	KK, %eax
	sall	$2 + BASE_SHIFT, %eax
	addl	%eax, BB
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
	prefetchw	-2 * SIZE(CO1)
	prefetchw	-2 * SIZE(CO1, LDC)
#else
	prefetchw	 1 * SIZE(CO1)
	prefetchw	 1 * SIZE(CO1, LDC)
#endif

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	sarl	$3, %eax
	je	.L45
	ALIGN_4

.L42:
	mulpd	%xmm0, %xmm2
#if defined(OPTERON) || defined(BARCELONA) || defined(BOBCAT) || defined(BARCELONA_OPTIMIZATION)
	prefetcht0 (PREFETCHSIZE  + 0) * SIZE(AA)
#endif
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

#if defined(OPTERON) || defined(BARCELONA) || defined(BOBCAT) || defined(BARCELONA_OPTIMIZATION)
	prefetcht0 (PREFETCHSIZE  + 8) * SIZE(AA)
#endif
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
	jne    .L42
	ALIGN_4

.L45:
#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L48
	ALIGN_3

.L46:
	mulpd	%xmm0, %xmm2
	mulpd	 2 * SIZE(BB), %xmm0
	addpd	%xmm2, %xmm4
	movapd	 4 * SIZE(BB), %xmm2
	addpd	%xmm0, %xmm5
	movapd	 2 * SIZE(AA), %xmm0

	addl	$2 * SIZE, AA
	addl	$4 * SIZE, BB
	decl	%eax
	jg	.L46
	ALIGN_4

.L48:
	addpd	%xmm6, %xmm4
	addpd	%xmm7, %xmm5

#if defined(LN) || defined(RT)
	movl	KK, %eax
#ifdef LN
	subl	$2, %eax
#else
	subl	$2, %eax
#endif

	movl	AORIG, AA
	movl	BORIG, B
	leal	BUFFER, BB

	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 2), AA
	leal	(B,  %eax, 2), B
	leal	(BB, %eax, 4), BB
#endif

#if defined(LN) || defined(LT)
	movapd	 %xmm4, %xmm0
	unpcklpd %xmm5, %xmm4
	unpckhpd %xmm5, %xmm0

	movapd	 0 * SIZE(B), %xmm2
	movapd	 2 * SIZE(B), %xmm3

	subpd	%xmm4,  %xmm2
	subpd	%xmm0,  %xmm3
#else
	movapd	 0 * SIZE(AA), %xmm0
	movapd	 2 * SIZE(AA), %xmm1

	subpd	%xmm4, %xmm0
	subpd	%xmm5, %xmm1
#endif

#ifdef LN
	movlpd	 3 * SIZE(AA), %xmm4
	movhpd	 3 * SIZE(AA), %xmm4
	mulpd	 %xmm4, %xmm3

	movlpd	 2 * SIZE(AA), %xmm4
	movhpd	 2 * SIZE(AA), %xmm4
	mulpd	 %xmm3, %xmm4
	subpd	 %xmm4, %xmm2

	movlpd	 0 * SIZE(AA), %xmm4
	movhpd	 0 * SIZE(AA), %xmm4
	mulpd	 %xmm4, %xmm2

#endif

#ifdef LT
	movlpd	 0 * SIZE(AA), %xmm4
	movhpd	 0 * SIZE(AA), %xmm4
	mulpd	 %xmm4, %xmm2

	movlpd	 1 * SIZE(AA), %xmm4
	movhpd	 1 * SIZE(AA), %xmm4
	mulpd	 %xmm2, %xmm4
	subpd	 %xmm4, %xmm3

	movlpd	 3 * SIZE(AA), %xmm4
	movhpd	 3 * SIZE(AA), %xmm4
	mulpd	 %xmm4, %xmm3
#endif

#ifdef RN
	movlpd	 0 * SIZE(B), %xmm4
	movhpd	 0 * SIZE(B), %xmm4
	mulpd	 %xmm4, %xmm0
	movlpd	 1 * SIZE(B), %xmm4
	movhpd	 1 * SIZE(B), %xmm4
	mulpd	 %xmm0, %xmm4
	subpd	 %xmm4, %xmm1

	movlpd	 3 * SIZE(B), %xmm4
	movhpd	 3 * SIZE(B), %xmm4
	mulpd	 %xmm4, %xmm1
#endif

#ifdef RT
	movlpd	 3 * SIZE(B), %xmm4
	movhpd	 3 * SIZE(B), %xmm4
	mulpd	 %xmm4, %xmm1
	movlpd	 2 * SIZE(B), %xmm4
	movhpd	 2 * SIZE(B), %xmm4
	mulpd	 %xmm1, %xmm4
	subpd	 %xmm4, %xmm0

	movlpd	 0 * SIZE(B), %xmm4
	movhpd	 0 * SIZE(B), %xmm4
	mulpd	 %xmm4, %xmm0
#endif

#if defined(LN) || defined(LT)
	movapd	%xmm2,   0 * SIZE(B)
	movapd	%xmm3,   2 * SIZE(B)

	movlpd	%xmm2,   0 * SIZE(BB)
	movlpd	%xmm2,   1 * SIZE(BB)
	movhpd	%xmm2,   2 * SIZE(BB)
	movhpd	%xmm2,   3 * SIZE(BB)
	movlpd	%xmm3,   4 * SIZE(BB)
	movlpd	%xmm3,   5 * SIZE(BB)
	movhpd	%xmm3,   6 * SIZE(BB)
	movhpd	%xmm3,   7 * SIZE(BB)
#else
	movapd	%xmm0,   0 * SIZE(AA)
	movapd	%xmm1,   2 * SIZE(AA)
#endif

#ifdef LN
	subl	$2 * SIZE, CO1
#endif

#if defined(LN) || defined(LT)
	movlpd	%xmm2, 0 * SIZE(CO1)
	movlpd	%xmm3, 1 * SIZE(CO1)
	movhpd	%xmm2, 0 * SIZE(CO1, LDC, 1)
	movhpd	%xmm3, 1 * SIZE(CO1, LDC, 1)
#else
	movlpd	%xmm0, 0 * SIZE(CO1)
	movhpd	%xmm0, 1 * SIZE(CO1)
	movlpd	%xmm1, 0 * SIZE(CO1, LDC, 1)
	movhpd	%xmm1, 1 * SIZE(CO1, LDC, 1)
#endif

#ifndef LN
	addl	$2 * SIZE, CO1
#endif

#if defined(LT) || defined(RN)
	movl	K,  %eax
	subl	KK, %eax
	leal	(,%eax, SIZE), %eax
	leal	(AA, %eax, 2), AA
#ifdef LT
	addl	$4 * SIZE, B
#endif
#endif

#ifdef LN
	subl	$2, KK
	movl	BORIG, B
#endif

#ifdef LT
	addl	$2, KK
#endif

#ifdef RT
	movl	K, %eax
	movl	BORIG, B
	sall	$1 + BASE_SHIFT, %eax
	addl	%eax, AORIG
#endif

	decl	%ebx			# i --
	jg	.L41
	ALIGN_4

.L59:
#ifdef LN
       movl	K, %eax
       leal	(, %eax, SIZE), %eax
       leal 	(B, %eax, 2), B
#endif

#if defined(LT) || defined(RN)
	movl	K,  %eax
	subl	KK, %eax
	leal	(,%eax, SIZE), %eax
	leal	(B,  %eax, 2), B
#endif

#ifdef RN
	addl	$2, KK
#endif

#ifdef RT
	subl	$2, KK
#endif
	ALIGN_4

.L60:
	testl	$1, N
	je	.L999

#ifdef LN
	movl	OFFSET, %eax
	addl	M, %eax
	movl	%eax, KK
#endif

	leal	BUFFER, BB

#ifdef RT
       movl	K, %eax
       sall	$BASE_SHIFT, %eax
       subl	%eax, B
#endif

#if defined(LN) || defined(RT)
	movl	KK, %eax
	movl	B, BORIG
	leal	(, %eax, SIZE), %eax
	leal	(B,  %eax, 1), B
	leal	(BB, %eax, 2), BB
#endif

#ifdef LT
	movl	OFFSET, %eax
	movl	%eax, KK
#endif

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	sarl	$3, %eax
	jle	.L65
	ALIGN_4

.L62:
#define COPYPREFETCH 40

	prefetchnta	(COPYPREFETCH) * SIZE(B)

	movq	 0 * SIZE(B), %mm0
	movq	 1 * SIZE(B), %mm1
	movq	 2 * SIZE(B), %mm2
	movq	 3 * SIZE(B), %mm3
	movq	 4 * SIZE(B), %mm4
	movq	 5 * SIZE(B), %mm5
	movq	 6 * SIZE(B), %mm6
	movq	 7 * SIZE(B), %mm7

	movq	%mm0,  0 * SIZE(BB)
	movq	%mm0,  1 * SIZE(BB)
	movq	%mm1,  2 * SIZE(BB)
	movq	%mm1,  3 * SIZE(BB)
	movq	%mm2,  4 * SIZE(BB)
	movq	%mm2,  5 * SIZE(BB)
	movq	%mm3,  6 * SIZE(BB)
	movq	%mm3,  7 * SIZE(BB)

	movq	%mm4,  8 * SIZE(BB)
	movq	%mm4,  9 * SIZE(BB)
	movq	%mm5, 10 * SIZE(BB)
	movq	%mm5, 11 * SIZE(BB)
	movq	%mm6, 12 * SIZE(BB)
	movq	%mm6, 13 * SIZE(BB)
	movq	%mm7, 14 * SIZE(BB)
	movq	%mm7, 15 * SIZE(BB)

	addl	$ 8 * SIZE, B
	addl	$16 * SIZE, BB
	decl	%eax
	jne	.L62
	ALIGN_2

.L65:
#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	andl	$7, %eax
	BRANCH
	jle	.L70
	ALIGN_2

.L66:
	movq	 0 * SIZE(B), %mm0

	movq	%mm0,  0 * SIZE(BB)
	movq	%mm0,  1 * SIZE(BB)

	addl	$1 * SIZE, B
	addl	$2 * SIZE, BB
	decl	%eax
	jne	.L66
	ALIGN_4

.L70:
#if defined(LT) || defined(RN)
	movl	A, AA
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
	testl	$1, %ebx	# i = (m >> 2)
	jle	.L80

#ifdef LN
       movl	K, %eax
       sall	$BASE_SHIFT, %eax
       subl	%eax, AORIG
#endif

#if defined(LN) || defined(RT)
	movl	KK, %eax
	movl	AORIG, AA
	leal	(AA, %eax, SIZE), AA
#endif

	leal	BUFFER, BB

#if defined(LN) || defined(RT)
	movl	KK, %eax
	sall	$1 + BASE_SHIFT, %eax
	addl	%eax, BB
#endif

	pxor	%xmm4, %xmm4
	pxor	%xmm5, %xmm5
	pxor	%xmm6, %xmm6
	pxor	%xmm7, %xmm7

	movlpd	 0 * SIZE(AA), %xmm0
	movlpd	 4 * SIZE(AA), %xmm1
	movlpd	 0 * SIZE(BB), %xmm2
	movlpd	 8 * SIZE(BB), %xmm3

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	sarl	$3, %eax
	je	.L85
	ALIGN_4

.L82:
	mulsd	%xmm0, %xmm2
	prefetcht0 (PREFETCHSIZE  + 0) * SIZE(AA)
	movlpd	 1 * SIZE(AA), %xmm0
	mulsd	 2 * SIZE(BB), %xmm0
	addsd	%xmm2, %xmm4
	movlpd	16 * SIZE(BB), %xmm2
	addsd	%xmm0, %xmm5
	movlpd	 2 * SIZE(AA), %xmm0
	mulsd	 4 * SIZE(BB), %xmm0
	addsd	%xmm0, %xmm6
	movlpd	 3 * SIZE(AA), %xmm0
	mulsd	 6 * SIZE(BB), %xmm0
	addsd	%xmm0, %xmm7
	movlpd	 8 * SIZE(AA), %xmm0
	mulsd	%xmm1, %xmm3
	movlpd	 5 * SIZE(AA), %xmm1
	mulsd	10 * SIZE(BB), %xmm1
	addsd	%xmm3, %xmm4
	movlpd	24 * SIZE(BB), %xmm3
	addsd	%xmm1, %xmm5
	movlpd	 6 * SIZE(AA), %xmm1
	mulsd	12 * SIZE(BB), %xmm1
	addsd	%xmm1, %xmm6
	movlpd	 7 * SIZE(AA), %xmm1
	mulsd	14 * SIZE(BB), %xmm1
	addsd	%xmm1, %xmm7
	movlpd	12 * SIZE(AA), %xmm1

	addl   $ 8 * SIZE, AA
	addl   $16 * SIZE, BB
	decl   %eax
	jne    .L82
	ALIGN_4

.L85:
#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L88

.L86:
	mulsd	%xmm0, %xmm2
	addsd	%xmm2, %xmm4
	movlpd	 2 * SIZE(BB), %xmm2
	movlpd	 1 * SIZE(AA), %xmm0

	addl	$1 * SIZE, AA
	addl	$2 * SIZE, BB
	decl	%eax
	jg	.L86
	ALIGN_4

.L88:
	addsd	%xmm5, %xmm4
	addsd	%xmm7, %xmm6
	addsd	%xmm6, %xmm4

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

	leal	(, %eax, SIZE), %eax
	addl	%eax, AA
	addl	%eax, B
	leal	(BB, %eax, 2), BB
#endif

#if defined(LN) || defined(LT)
	movlpd	 0 * SIZE(B), %xmm2
	subsd	%xmm4, %xmm2
#else
	movlpd	 0 * SIZE(AA), %xmm0
	subsd	%xmm4, %xmm0
#endif

#ifdef LN
	movlpd	 0 * SIZE(AA), %xmm4
	mulsd	 %xmm4, %xmm2
#endif

#ifdef LT
	movlpd	 0 * SIZE(AA), %xmm4
	mulsd	 %xmm4, %xmm2
#endif

#ifdef RN
	movlpd	 0 * SIZE(B), %xmm4
	mulsd	 %xmm4, %xmm0
#endif

#ifdef RT
	movlpd	 0 * SIZE(B), %xmm4
	mulsd	 %xmm4, %xmm0
#endif

#if defined(LN) || defined(LT)
	movlpd	%xmm2,   0 * SIZE(B)

	movlpd	%xmm2,   0 * SIZE(BB)
	movlpd	%xmm2,   1 * SIZE(BB)
#else
	movlpd	%xmm0,   0 * SIZE(AA)
#endif

#ifdef LN
	subl	$1 * SIZE, CO1
#endif

#if defined(LN) || defined(LT)
	movlpd	%xmm2, 0 * SIZE(CO1)
#else
	movlpd	%xmm0, 0 * SIZE(CO1)
#endif

#ifndef LN
	addl	$1 * SIZE, CO1
#endif

#if defined(LT) || defined(RN)
	movl	K,  %eax
	subl	KK, %eax
	leal	(AA,%eax, SIZE), AA
#ifdef LT
	addl	$1 * SIZE, B
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
	sall	$BASE_SHIFT, %eax
	addl	%eax, AORIG
#endif
	ALIGN_4

.L80:
	movl	M,  %ebx
	sarl	$1, %ebx	# i = (m >> 2)
	jle	.L99
	ALIGN_4

.L71:
#ifdef LN
       movl	K, %eax
       sall	$1 + BASE_SHIFT, %eax
       subl	%eax, AORIG
#endif

#if defined(LN) || defined(RT)
	movl	KK, %eax
	movl	AORIG, AA
	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 2), AA
#endif

	leal	BUFFER, BB

#if defined(LN) || defined(RT)
	movl	KK, %eax
	sall	$1 + BASE_SHIFT, %eax
	addl	%eax, BB
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
	prefetchw	-2 * SIZE(CO1)
#else
	prefetchw	 1 * SIZE(CO1)
#endif

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	sarl	$3, %eax
	je	.L75
	ALIGN_4

.L72:
	mulpd	%xmm0, %xmm2
	addpd	%xmm2, %xmm4
	prefetcht0 (PREFETCHSIZE  + 0) * SIZE(AA)
	movapd	16 * SIZE(BB), %xmm2

	movapd	 2 * SIZE(AA), %xmm0
	mulpd	 2 * SIZE(BB), %xmm0
	addpd	%xmm0, %xmm4
	movapd	 4 * SIZE(AA), %xmm0
	mulpd	 4 * SIZE(BB), %xmm0
	addpd	%xmm0, %xmm4
	movapd	 6 * SIZE(AA), %xmm0
	mulpd	 6 * SIZE(BB), %xmm0
	addpd	%xmm0, %xmm4

	movapd	16 * SIZE(AA), %xmm0
	prefetcht0 (PREFETCHSIZE  + 8) * SIZE(AA)
	mulpd	%xmm1, %xmm3
	addpd	%xmm3, %xmm4
	movapd	24 * SIZE(BB), %xmm3

	movapd	10 * SIZE(AA), %xmm1
	mulpd	10 * SIZE(BB), %xmm1
	addpd	%xmm1, %xmm4
	movapd	12 * SIZE(AA), %xmm1
	mulpd	12 * SIZE(BB), %xmm1
	addpd	%xmm1, %xmm4
	movapd	14 * SIZE(AA), %xmm1
	mulpd	14 * SIZE(BB), %xmm1
	addpd	%xmm1, %xmm4
	movapd	24 * SIZE(AA), %xmm1

	addl   $16 * SIZE, AA
	addl   $16 * SIZE, BB
	decl   %eax
	jne    .L72
	ALIGN_4

.L75:
#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L78
	ALIGN_3

.L76:
	mulpd	%xmm0, %xmm2
	addpd	%xmm2, %xmm4
	movapd	 2 * SIZE(AA), %xmm0
	movapd	 2 * SIZE(BB), %xmm2

	addl	$2 * SIZE, AA
	addl	$2 * SIZE, BB
	decl	%eax
	jg	.L76
	ALIGN_4

.L78:
#if defined(LN) || defined(RT)
	movl	KK, %eax
#ifdef LN
	subl	$2, %eax
#else
	subl	$1, %eax
#endif

	movl	AORIG, AA
	movl	BORIG, B
	leal	BUFFER, BB

	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 2), AA
	leal	(B,  %eax, 1), B
	leal	(BB, %eax, 2), BB
#endif

#if defined(LN) || defined(LT)
	movapd	 0 * SIZE(B), %xmm2

	subpd	%xmm4,  %xmm2
#else
	movapd	 0 * SIZE(AA), %xmm0

	subpd	%xmm4, %xmm0
#endif

#ifdef LN
	movapd	 %xmm2, %xmm3
	unpckhpd %xmm3, %xmm3

	movlpd	 3 * SIZE(AA), %xmm4
	mulsd	 %xmm4, %xmm3

	movlpd	 2 * SIZE(AA), %xmm4
	mulsd	 %xmm3, %xmm4
	subsd	 %xmm4, %xmm2

	movlpd	 0 * SIZE(AA), %xmm4
	mulsd	 %xmm4, %xmm2

	unpcklpd %xmm3, %xmm2
#endif

#ifdef LT
	movapd	 %xmm2, %xmm3
	unpckhpd %xmm3, %xmm3

	movlpd	 0 * SIZE(AA), %xmm4
	mulsd	 %xmm4, %xmm2

	movlpd	 1 * SIZE(AA), %xmm4
	mulsd	 %xmm2, %xmm4
	subsd	 %xmm4, %xmm3

	movlpd	 3 * SIZE(AA), %xmm4
	mulsd	 %xmm4, %xmm3

	unpcklpd %xmm3, %xmm2
#endif

#ifdef RN
	movlpd	 0 * SIZE(B), %xmm4
	movhpd	 0 * SIZE(B), %xmm4
	mulpd	 %xmm4, %xmm0
#endif

#ifdef RT
	movlpd	 0 * SIZE(B), %xmm4
	movhpd	 0 * SIZE(B), %xmm4
	mulpd	 %xmm4, %xmm0
#endif

#if defined(LN) || defined(LT)
	movapd	%xmm2,   0 * SIZE(B)

	movlpd	%xmm2,   0 * SIZE(BB)
	movlpd	%xmm2,   1 * SIZE(BB)
	movhpd	%xmm2,   2 * SIZE(BB)
	movhpd	%xmm2,   3 * SIZE(BB)
#else
	movapd	%xmm0,   0 * SIZE(AA)
#endif

#ifdef LN
	subl	$2 * SIZE, CO1
#endif

#if defined(LN) || defined(LT)
	movlpd	%xmm2, 0 * SIZE(CO1)
	movhpd	%xmm2, 1 * SIZE(CO1)
#else
	movlpd	%xmm0, 0 * SIZE(CO1)
	movhpd	%xmm0, 1 * SIZE(CO1)
#endif

#ifndef LN
	addl	$2 * SIZE, CO1
#endif

#if defined(LT) || defined(RN)
	movl	K,  %eax
	subl	KK, %eax
	leal	(,%eax, SIZE), %eax
	leal	(AA, %eax, 2), AA
#ifdef LT
	addl	$2 * SIZE, B
#endif
#endif

#ifdef LN
	subl	$2, KK
	movl	BORIG, B
#endif

#ifdef LT
	addl	$2, KK
#endif

#ifdef RT
	movl	K, %eax
	movl	BORIG, B
	sall	$1 + BASE_SHIFT, %eax
	addl	%eax, AORIG
#endif

	decl	%ebx			# i --
	jg	.L71
	ALIGN_4

.L99:
#ifdef LN
       movl	K, %eax
       leal	(B, %eax, SIZE), B
#endif

#if defined(LT) || defined(RN)
	movl	K,  %eax
	subl	KK, %eax
	leal	(B,%eax, SIZE), B
#endif

#ifdef RN
	addl	$1, KK
#endif

#ifdef RT
	subl	$1, KK
#endif
	ALIGN_4

.L999:
	movl	OLD_STACK, %esp

	EMMS

	popl	%ebx
	popl	%esi
	popl	%edi
	popl	%ebp
	ret

	EPILOGUE
