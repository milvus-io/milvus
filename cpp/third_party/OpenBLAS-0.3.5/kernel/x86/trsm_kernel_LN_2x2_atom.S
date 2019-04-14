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
#define ARGS	16

#define M	 4 + STACK + ARGS(%esp)
#define N	 8 + STACK + ARGS(%esp)
#define K	12 + STACK + ARGS(%esp)
#define ALPHA	16 + STACK + ARGS(%esp)
#define A	24 + STACK + ARGS(%esp)
#define ARG_B	28 + STACK + ARGS(%esp)
#define C	32 + STACK + ARGS(%esp)
#define ARG_LDC	36 + STACK + ARGS(%esp)
#define OFFSET	40 + STACK + ARGS(%esp)

#define J	 0 + STACK(%esp)
#define KK	 4 + STACK(%esp)
#define KKK	 8 + STACK(%esp)
#define AORIG	12 + STACK(%esp)

#define PREFETCH     prefetcht0
#define PREFETCHSIZE 84

#define AA	%edx
#define BB	%ecx
#define LDC	%ebp
#define B	%edi
#define CO1	%esi

	PROLOGUE

	subl	$ARGS, %esp

	pushl	%ebp
	pushl	%edi
	pushl	%esi
	pushl	%ebx

	PROFCODE

	movl	ARG_B,   B
	movl	ARG_LDC, LDC

	movl	OFFSET, %eax
#ifdef RN
	negl	%eax
#endif
	movl	%eax, KK

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

#ifdef RT
	movl	N, %eax
	subl	OFFSET, %eax
	movl	%eax, KK
#endif

	movl	N,  %eax
	sarl	$1, %eax
	movl	%eax, J
	jle	.L30
	ALIGN_2

.L10:
#if defined(LT) || defined(RN)
	movl	A, AA
#else
	movl	A, %eax
	movl	%eax, AORIG
#endif

#ifdef RT
	movl	K, %eax
	sall	$1 + BASE_SHIFT, %eax
	subl	%eax, B
#endif

	leal	(, LDC, 2), %eax

#ifdef RT
	subl	%eax, C
#endif
	movl	C, CO1
#ifndef RT
	addl	%eax, C
#endif

#ifdef LN
	movl	OFFSET, %eax
	addl	M, %eax
	movl	%eax, KK
#endif

#ifdef LT
	movl	OFFSET, %eax
	movl	%eax, KK
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

	movl	B, BB

#if defined(LN) || defined(RT)
	movl	KK, %eax
	sall	$1 + BASE_SHIFT, %eax
	addl	%eax, BB
#endif

	movsd	 0 * SIZE(AA), %xmm0
	xorps	%xmm2, %xmm2
	xorps	%xmm3, %xmm3
	xorps	%xmm4, %xmm4
	xorps	%xmm5, %xmm5

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	sarl	$2, %eax
	je	.L25
	ALIGN_4

.L22:
	addsd	%xmm2, %xmm4
	movsd	 0 * SIZE(BB), %xmm2
	addsd	%xmm3, %xmm5
	movsd	 1 * SIZE(BB), %xmm3

	mulsd	%xmm0, %xmm2
	PREFETCH  (PREFETCHSIZE + 0) * SIZE(AA)
	mulsd	%xmm0, %xmm3
	movsd	 1 * SIZE(AA), %xmm0

	addsd	%xmm2, %xmm4
	movsd	 2 * SIZE(BB), %xmm2
	addsd	%xmm3, %xmm5
	movsd	 3 * SIZE(BB), %xmm3

	mulsd	%xmm0, %xmm2
	mulsd	%xmm0, %xmm3
	movsd	 2 * SIZE(AA), %xmm0

	addsd	%xmm2, %xmm4
	movsd	 4 * SIZE(BB), %xmm2
	addsd	%xmm3, %xmm5
	movsd	 5 * SIZE(BB), %xmm3

	mulsd	%xmm0, %xmm2
	mulsd	%xmm0, %xmm3
	movsd	 3 * SIZE(AA), %xmm0

	addsd	%xmm2, %xmm4
	movsd	 6 * SIZE(BB), %xmm2
	addsd	%xmm3, %xmm5
	movsd	 7 * SIZE(BB), %xmm3

	mulsd	%xmm0, %xmm2
	mulsd	%xmm0, %xmm3
	movsd	 4 * SIZE(AA), %xmm0

	addl   $4 * SIZE, AA
	addl   $8 * SIZE, BB
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
	andl	$3, %eax		# if (k & 1)
	BRANCH
	je .L28
	ALIGN_3

.L26:
	addsd	%xmm2, %xmm4
	movsd	 0 * SIZE(BB), %xmm2
	addsd	%xmm3, %xmm5
	movsd	 1 * SIZE(BB), %xmm3

	mulsd	%xmm0, %xmm2
	mulsd	%xmm0, %xmm3
	movsd	 1 * SIZE(AA), %xmm0

	addl	$1 * SIZE, AA
	addl	$2 * SIZE, BB
	decl	%eax
	jg	.L26
	ALIGN_4

.L28:
	addsd	%xmm2, %xmm4
	addsd	%xmm3, %xmm5

#if defined(LN) || defined(RT)
	movl	KK, %eax
#ifdef LN
	subl	$1, %eax
#else
	subl	$2, %eax
#endif

	movl	AORIG, AA

	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 1), AA
	leal	(B,  %eax, 2), BB
#endif

#if defined(LN) || defined(LT)
	movsd	 0 * SIZE(BB), %xmm0
	movsd	 1 * SIZE(BB), %xmm1

	subsd	%xmm4,  %xmm0
	subsd	%xmm5,  %xmm1
#else
	movsd	 0 * SIZE(AA), %xmm0
	movsd	 1 * SIZE(AA), %xmm1

	subsd	%xmm4,  %xmm0
	subsd	%xmm5,  %xmm1
#endif

#if defined(LN) || defined(LT)
	movsd	 0 * SIZE(AA), %xmm7

	mulsd	 %xmm7, %xmm0
	mulsd	 %xmm7, %xmm1
#endif

#ifdef RN
	movsd	 0 * SIZE(BB), %xmm4
	mulsd	 %xmm4, %xmm0
	movsd	 1 * SIZE(BB), %xmm5
	movaps	 %xmm5, %xmm6
	movsd	 3 * SIZE(BB), %xmm7
	mulsd	 %xmm0, %xmm5
	subsd	 %xmm5, %xmm1
	mulsd	 %xmm7, %xmm1
#endif

#ifdef RT
	movsd	 3 * SIZE(BB), %xmm4
	mulsd	 %xmm4, %xmm1
	movsd	 2 * SIZE(BB), %xmm5
	movaps	 %xmm5, %xmm6
	movsd	 0 * SIZE(BB), %xmm7
	mulsd	 %xmm1, %xmm5
	subsd	 %xmm5, %xmm0
	mulsd	 %xmm7, %xmm0
#endif

#if defined(LN) || defined(LT)
	movsd	%xmm0,   0 * SIZE(BB)
	movsd	%xmm1,   1 * SIZE(BB)
#else
	movsd	%xmm0,   0 * SIZE(AA)
	movsd	%xmm1,   1 * SIZE(AA)
#endif

#ifdef LN
	subl	$1 * SIZE, CO1
#endif

	movsd	%xmm0, 0 * SIZE(CO1)
	movsd	%xmm1, 0 * SIZE(CO1, LDC)

#ifndef LN
	addl	$1 * SIZE, CO1
#endif

#if defined(LT) || defined(RN)
	movl	K,  %eax
	subl	KK, %eax
	leal	(,%eax, SIZE), %eax
	leal	(AA, %eax, 1), AA
	leal	(BB, %eax, 2), BB
#endif

#ifdef LN
	subl	$1, KK
#endif

#ifdef LT
	addl	$1, KK
#endif

#ifdef RT
	movl	K, %eax
	sall	$BASE_SHIFT, %eax
	addl	%eax, AORIG
#endif
	ALIGN_4

.L20:
	movl	M,  %ebx
	sarl	$1, %ebx
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

	movl	B, BB

#if defined(LN) || defined(RT)
	movl	KK, %eax
	sall	$1 + BASE_SHIFT, %eax
	addl	%eax, BB
#endif

	movsd	 0 * SIZE(AA), %xmm0
	xorps	%xmm2, %xmm2
	xorps	%xmm3, %xmm3

	xorps	%xmm4, %xmm4
	prefetcht0	3 * SIZE(CO1)
	xorps	%xmm5, %xmm5
	prefetcht0	3 * SIZE(CO1, LDC)
	xorps	%xmm6, %xmm6
	xorps	%xmm7, %xmm7

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	sarl	$2, %eax
	je	.L15
	ALIGN_4

.L12:
	PREFETCH  (PREFETCHSIZE + 0) * SIZE(AA)

	addsd	%xmm2, %xmm6
	movsd	 1 * SIZE(AA), %xmm2
	movaps	%xmm0, %xmm1
	mulsd	 0 * SIZE(BB), %xmm0
	addsd	%xmm3, %xmm7
	mulsd	 1 * SIZE(BB), %xmm1

	addsd	%xmm0, %xmm4
	movsd	 2 * SIZE(AA), %xmm0
	movaps	%xmm2, %xmm3
	mulsd	 0 * SIZE(BB), %xmm2
	addsd	%xmm1, %xmm5
	mulsd	 1 * SIZE(BB), %xmm3

	addsd	%xmm2, %xmm6
	movsd	 3 * SIZE(AA), %xmm2
	movaps	%xmm0, %xmm1
	mulsd	 2 * SIZE(BB), %xmm0
	addsd	%xmm3, %xmm7
	mulsd	 3 * SIZE(BB), %xmm1

	addsd	%xmm0, %xmm4
	movsd	 4 * SIZE(AA), %xmm0
	movaps	%xmm2, %xmm3
	mulsd	 2 * SIZE(BB), %xmm2
	addsd	%xmm1, %xmm5
	mulsd	 3 * SIZE(BB), %xmm3

	addsd	%xmm2, %xmm6
	movsd	 5 * SIZE(AA), %xmm2
	movaps	%xmm0, %xmm1
	mulsd	 4 * SIZE(BB), %xmm0
	addsd	%xmm3, %xmm7
	mulsd	 5 * SIZE(BB), %xmm1

	addsd	%xmm0, %xmm4
	movsd	 6 * SIZE(AA), %xmm0
	movaps	%xmm2, %xmm3
	mulsd	 4 * SIZE(BB), %xmm2
	addsd	%xmm1, %xmm5
	mulsd	 5 * SIZE(BB), %xmm3

	addsd	%xmm2, %xmm6
	movsd	 7 * SIZE(AA), %xmm2
	movaps	%xmm0, %xmm1
	mulsd	 6 * SIZE(BB), %xmm0
	addsd	%xmm3, %xmm7
	mulsd	 7 * SIZE(BB), %xmm1

	addsd	%xmm0, %xmm4
	movsd	 8 * SIZE(AA), %xmm0
	movaps	%xmm2, %xmm3
	mulsd	 6 * SIZE(BB), %xmm2
	addsd	%xmm1, %xmm5
	mulsd	 7 * SIZE(BB), %xmm3

	addl   $8 * SIZE, BB
	addl   $8 * SIZE, AA
	decl   %eax
	jne    .L12
	ALIGN_4

.L15:
#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	andl	$3, %eax		# if (k & 1)
	BRANCH
	je .L18
	ALIGN_3

.L16:
	addsd	%xmm2, %xmm6
	movsd	 1 * SIZE(AA), %xmm2
	movaps	%xmm0, %xmm1
	mulsd	 0 * SIZE(BB), %xmm0
	addsd	%xmm3, %xmm7
	mulsd	 1 * SIZE(BB), %xmm1

	addsd	%xmm0, %xmm4
	movsd	 2 * SIZE(AA), %xmm0
	movaps	%xmm2, %xmm3
	mulsd	 0 * SIZE(BB), %xmm2
	addsd	%xmm1, %xmm5
	mulsd	 1 * SIZE(BB), %xmm3

	addl	$2 * SIZE, AA
	addl	$2 * SIZE, BB
	decl	%eax
	jg	.L16
	ALIGN_4

.L18:
	addsd	%xmm2, %xmm6
	addsd	%xmm3, %xmm7

#if defined(LN) || defined(RT)
	movl	KK, %eax
#ifdef LN
	subl	$2, %eax
#else
	subl	$2, %eax
#endif

	movl	AORIG, AA

	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 2), AA
	leal	(B,  %eax, 2), BB
#endif

#if defined(LN) || defined(LT)
	movsd	 0 * SIZE(BB), %xmm0
	movsd	 1 * SIZE(BB), %xmm1
	movsd	 2 * SIZE(BB), %xmm2
	movsd	 3 * SIZE(BB), %xmm3

	subsd	%xmm4,  %xmm0
	subsd	%xmm5,  %xmm1
	subsd	%xmm6,  %xmm2
	subsd	%xmm7,  %xmm3
#else
	movsd	 0 * SIZE(AA), %xmm0
	movsd	 1 * SIZE(AA), %xmm2
	movsd	 2 * SIZE(AA), %xmm1
	movsd	 3 * SIZE(AA), %xmm3

	subsd	%xmm4,  %xmm0
	subsd	%xmm6,  %xmm2
	subsd	%xmm5,  %xmm1
	subsd	%xmm7,  %xmm3
#endif

#ifdef LN
	movsd	 3 * SIZE(AA), %xmm4
	mulsd	 %xmm4, %xmm2
	movsd	 2 * SIZE(AA), %xmm5
	mulsd	 %xmm4, %xmm3
	movsd	 0 * SIZE(AA), %xmm7

	movaps	 %xmm5, %xmm6
	mulsd	 %xmm2, %xmm5
	mulsd	 %xmm3, %xmm6
	subsd	 %xmm5, %xmm0
	subsd	 %xmm6, %xmm1
	mulsd	 %xmm7, %xmm0
	mulsd	 %xmm7, %xmm1
#endif

#ifdef LT
	movsd	 0 * SIZE(AA), %xmm4
	mulsd	 %xmm4, %xmm0
	movsd	 1 * SIZE(AA), %xmm5
	mulsd	 %xmm4, %xmm1
	movsd	 3 * SIZE(AA), %xmm7

	movaps	 %xmm5, %xmm6
	mulsd	 %xmm0, %xmm5
	mulsd	 %xmm1, %xmm6
	subsd	 %xmm5, %xmm2
	subsd	 %xmm6, %xmm3
	mulsd	 %xmm7, %xmm2
	mulsd	 %xmm7, %xmm3
#endif

#ifdef RN
	movsd	 0 * SIZE(BB), %xmm4
	mulsd	 %xmm4, %xmm0
	movsd	 1 * SIZE(BB), %xmm5
	mulsd	 %xmm4, %xmm2
	movsd	 3 * SIZE(BB), %xmm7

	movaps	 %xmm5, %xmm6
	mulsd	 %xmm0, %xmm5
	mulsd	 %xmm2, %xmm6
	subsd	 %xmm5, %xmm1
	subsd	 %xmm6, %xmm3
	mulsd	 %xmm7, %xmm1
	mulsd	 %xmm7, %xmm3
#endif

#ifdef RT
	movsd	 3 * SIZE(BB), %xmm4
	mulsd	 %xmm4, %xmm1
	movsd	 2 * SIZE(BB), %xmm5
	mulsd	 %xmm4, %xmm3
	movsd	 0 * SIZE(BB), %xmm7

	movaps	 %xmm5, %xmm6
	mulsd	 %xmm1, %xmm5
	mulsd	 %xmm3, %xmm6
	subsd	 %xmm5, %xmm0
	subsd	 %xmm6, %xmm2
	mulsd	 %xmm7, %xmm0
	mulsd	 %xmm7, %xmm2
#endif

#if defined(LN) || defined(LT)
	movsd	%xmm0,   0 * SIZE(BB)
	movsd	%xmm1,   1 * SIZE(BB)
	movsd	%xmm2,   2 * SIZE(BB)
	movsd	%xmm3,   3 * SIZE(BB)
#else
	movsd	%xmm0,   0 * SIZE(AA)
	movsd	%xmm2,   1 * SIZE(AA)
	movsd	%xmm1,   2 * SIZE(AA)
	movsd	%xmm3,   3 * SIZE(AA)
#endif

#ifdef LN
	subl	$2 * SIZE, CO1
#endif

	movsd	%xmm0, 0 * SIZE(CO1)
	movsd	%xmm2, 1 * SIZE(CO1)
	movsd	%xmm1, 0 * SIZE(CO1, LDC)
	movsd	%xmm3, 1 * SIZE(CO1, LDC)

#ifndef LN
	addl	$2 * SIZE, CO1
#endif

#if defined(LT) || defined(RN)
	movl	K,  %eax
	subl	KK, %eax
	leal	(,%eax, SIZE), %eax
	leal	(AA, %eax, 2), AA
	leal	(BB, %eax, 2), BB
#endif

#ifdef LN
	subl	$2, KK
#endif

#ifdef LT
	addl	$2, KK
#endif

#ifdef RT
	movl	K, %eax
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
       leal 	(B, %eax, 2), B
#endif

#if defined(LT) || defined(RN)
	movl	BB, B
#endif

#ifdef RN
	addl	$2, KK
#endif

#ifdef RT
	subl	$2, KK
#endif

	decl	J			# j --
	jg	.L10
	ALIGN_4

.L30:
	testl	$1, N
	je	.L999

#if defined(LT) || defined(RN)
	movl	A, AA
#else
	movl	A, %eax
	movl	%eax, AORIG
#endif

#ifdef RT
	movl	K, %eax
	sall	$BASE_SHIFT, %eax
	subl	%eax, B
#endif

#ifdef RT
	subl	LDC, C
#endif
	movl	C, CO1
#ifndef RT
	addl	LDC, C
#endif

#ifdef LN
	movl	OFFSET, %eax
	addl	M, %eax
	movl	%eax, KK
#endif

#ifdef LT
	movl	OFFSET, %eax
	movl	%eax, KK
#endif

	movl	M,  %ebx
	testl	$1, %ebx	# i = (m >> 2)
	jle	.L40

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

	movl	B, BB

#if defined(LN) || defined(RT)
	movl	KK, %eax
	sall	$BASE_SHIFT, %eax
	addl	%eax, BB
#endif

	movsd	 0 * SIZE(AA), %xmm0
	xorps	%xmm2, %xmm2
	movsd	 0 * SIZE(BB), %xmm2
	xorps	%xmm3, %xmm3
	xorps	%xmm4, %xmm4
	xorps	%xmm5, %xmm5

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	sarl	$2, %eax
	je	.L45
	ALIGN_4

.L42:
	mulsd	%xmm0, %xmm2
	movsd	 1 * SIZE(AA), %xmm0
	addsd	%xmm2, %xmm4
	movsd	 1 * SIZE(BB), %xmm2

	mulsd	%xmm0, %xmm2
	movsd	 2 * SIZE(AA), %xmm0
	addsd	%xmm2, %xmm5
	movsd	 2 * SIZE(BB), %xmm2

	mulsd	%xmm0, %xmm2
	movsd	 3 * SIZE(AA), %xmm0
	addsd	%xmm2, %xmm4
	movsd	 3 * SIZE(BB), %xmm2

	mulsd	%xmm0, %xmm2
	movsd	 4 * SIZE(AA), %xmm0
	addsd	%xmm2, %xmm5
	movsd	 4 * SIZE(BB), %xmm2

	addl   $4 * SIZE, AA
	addl   $4 * SIZE, BB
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
	andl	$3, %eax		# if (k & 1)
	BRANCH
	je .L48
	ALIGN_3

.L46:
	mulsd	%xmm0, %xmm2
	movsd	 1 * SIZE(AA), %xmm0
	addsd	%xmm2, %xmm4
	movsd	 1 * SIZE(BB), %xmm2

	addl	$1 * SIZE, AA
	addl	$1 * SIZE, BB
	decl	%eax
	jg	.L46
	ALIGN_4

.L48:
	addsd	%xmm5, %xmm4

#if defined(LN) || defined(RT)
	movl	KK, %eax
#ifdef LN
	subl	$1, %eax
#else
	subl	$1, %eax
#endif

	movl	AORIG, AA

	leal	(, %eax, SIZE), %eax
	addl	%eax, AA
	leal	(B,  %eax, 1), BB
#endif

#if defined(LN) || defined(LT)
	movsd	 0 * SIZE(BB), %xmm0
	subsd	%xmm4,  %xmm0
#else
	movsd	 0 * SIZE(AA), %xmm0
	subsd	%xmm4,  %xmm0
#endif

#if defined(LN) || defined(LT)
	mulsd	 0 * SIZE(AA), %xmm0
#endif

#if defined(RN) || defined(RT)
	mulsd	 0 * SIZE(BB), %xmm0
#endif

#if defined(LN) || defined(LT)
	movsd	%xmm0,   0 * SIZE(BB)
#else
	movsd	%xmm0,   0 * SIZE(AA)
#endif

#ifdef LN
	subl	$1 * SIZE, CO1
#endif

	movsd	%xmm0, 0 * SIZE(CO1)

#ifndef LN
	addl	$1 * SIZE, CO1
#endif

#if defined(LT) || defined(RN)
	movl	K,  %eax
	subl	KK, %eax
	leal	(,%eax, SIZE), %eax
	addl	%eax, AA
	addl	%eax, BB
#endif

#ifdef LN
	subl	$1, KK
#endif

#ifdef LT
	addl	$1, KK
#endif

#ifdef RT
	movl	K, %eax
	sall	$BASE_SHIFT, %eax
	addl	%eax, AORIG
#endif
	ALIGN_4

.L40:
	movl	M,  %ebx
	sarl	$1, %ebx
	jle	.L49
	ALIGN_4

.L31:
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

	movl	B, BB

#if defined(LN) || defined(RT)
	movl	KK, %eax
	sall	$BASE_SHIFT, %eax
	addl	%eax, BB
#endif

	movsd	 0 * SIZE(BB), %xmm1
	xorps	%xmm0, %xmm0
	prefetcht0	3 * SIZE(CO1)
	xorps	%xmm2, %xmm2
	xorps	%xmm4, %xmm4
	xorps	%xmm6, %xmm6

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	sarl	$2, %eax
	je	.L35
	ALIGN_4

.L32:
	PREFETCH  (PREFETCHSIZE + 0) * SIZE(AA)

	addsd	%xmm0, %xmm4
	movsd	 0 * SIZE(AA), %xmm0
	addsd	%xmm2, %xmm6
	movsd	 1 * SIZE(AA), %xmm2
	mulsd	 %xmm1, %xmm0
	mulsd	 %xmm1, %xmm2
	movsd	 1 * SIZE(BB), %xmm1

	addsd	%xmm0, %xmm4
	movsd	 2 * SIZE(AA), %xmm0
	addsd	%xmm2, %xmm6
	movsd	 3 * SIZE(AA), %xmm2
	mulsd	 %xmm1, %xmm0
	mulsd	 %xmm1, %xmm2
	movsd	 2 * SIZE(BB), %xmm1

	addsd	%xmm0, %xmm4
	movsd	 4 * SIZE(AA), %xmm0
	addsd	%xmm2, %xmm6
	movsd	 5 * SIZE(AA), %xmm2
	mulsd	 %xmm1, %xmm0
	mulsd	 %xmm1, %xmm2
	movsd	 3 * SIZE(BB), %xmm1

	addsd	%xmm0, %xmm4
	movsd	 6 * SIZE(AA), %xmm0
	addsd	%xmm2, %xmm6
	movsd	 7 * SIZE(AA), %xmm2
	mulsd	 %xmm1, %xmm0
	mulsd	 %xmm1, %xmm2
	movsd	 4 * SIZE(BB), %xmm1

	addl   $8 * SIZE, AA
	addl   $4 * SIZE, BB
	decl   %eax
	jne    .L32
	ALIGN_4

.L35:
#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	andl	$3, %eax		# if (k & 1)
	BRANCH
	je .L38
	ALIGN_3

.L36:
	addsd	%xmm0, %xmm4
	movsd	 0 * SIZE(AA), %xmm0
	addsd	%xmm2, %xmm6
	movsd	 1 * SIZE(AA), %xmm2
	mulsd	 %xmm1, %xmm0
	mulsd	 %xmm1, %xmm2
	movsd	 1 * SIZE(BB), %xmm1

	addl	$2 * SIZE, AA
	addl	$1 * SIZE, BB
	decl	%eax
	jg	.L36
	ALIGN_4

.L38:
	addsd	%xmm0, %xmm4
	addsd	%xmm2, %xmm6

#if defined(LN) || defined(RT)
	movl	KK, %eax
#ifdef LN
	subl	$2, %eax
#else
	subl	$1, %eax
#endif

	movl	AORIG, AA

	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 2), AA
	leal	(B,  %eax, 1), BB
#endif

#if defined(LN) || defined(LT)
	movsd	 0 * SIZE(BB), %xmm0
	movsd	 1 * SIZE(BB), %xmm2

	subsd	%xmm4,  %xmm0
	subsd	%xmm6,  %xmm2
#else
	movsd	 0 * SIZE(AA), %xmm0
	movsd	 1 * SIZE(AA), %xmm2

	subsd	%xmm4,  %xmm0
	subsd	%xmm6,  %xmm2
#endif

#ifdef LN
	movsd	 3 * SIZE(AA), %xmm4
	mulsd	 %xmm4, %xmm2
	movsd	 2 * SIZE(AA), %xmm5
	mulsd	 %xmm2, %xmm5
	movsd	 0 * SIZE(AA), %xmm7
	subsd	 %xmm5, %xmm0
	mulsd	 %xmm7, %xmm0
#endif

#ifdef LT
	movsd	 0 * SIZE(AA), %xmm4
	mulsd	 %xmm4, %xmm0
	movsd	 1 * SIZE(AA), %xmm5
	mulsd	 %xmm0, %xmm5
	movsd	 3 * SIZE(AA), %xmm7
	subsd	 %xmm5, %xmm2
	mulsd	 %xmm7, %xmm2
#endif

#if defined(RN) || defined(RT)
	movsd	 0 * SIZE(BB), %xmm4
	mulsd	 %xmm4, %xmm0
	mulsd	 %xmm4, %xmm2
#endif


#if defined(LN) || defined(LT)
	movsd	%xmm0,   0 * SIZE(BB)
	movsd	%xmm2,   1 * SIZE(BB)
#else
	movsd	%xmm0,   0 * SIZE(AA)
	movsd	%xmm2,   1 * SIZE(AA)
#endif

#ifdef LN
	subl	$2 * SIZE, CO1
#endif

	movsd	%xmm0, 0 * SIZE(CO1)
	movsd	%xmm2, 1 * SIZE(CO1)

#ifndef LN
	addl	$2 * SIZE, CO1
#endif

#if defined(LT) || defined(RN)
	movl	K,  %eax
	subl	KK, %eax
	leal	(,%eax, SIZE), %eax
	leal	(AA, %eax, 2), AA
	addl	%eax, BB
#endif

#ifdef LN
	subl	$2, KK
#endif

#ifdef LT
	addl	$2, KK
#endif

#ifdef RT
	movl	K, %eax
	sall	$1 + BASE_SHIFT, %eax
	addl	%eax, AORIG
#endif

	decl	%ebx			# i --
	jg	.L31
	ALIGN_4

.L49:
#ifdef LN
       movl	K, %eax
       leal	(B, %eax, SIZE), B
#endif

#if defined(LT) || defined(RN)
	movl	BB, B
#endif

#ifdef RN
	addl	$1, KK
#endif

#ifdef RT
	subl	$1, KK
#endif
	ALIGN_4

.L999:
	popl	%ebx
	popl	%esi
	popl	%edi
	popl	%ebp

	addl	$ARGS, %esp
	ret

	EPILOGUE
